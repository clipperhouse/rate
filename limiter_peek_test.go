package rate

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/clipperhouse/ntime"
	"github.com/stretchr/testify/require"
)

func TestLimiter_Peek_NeverPersists(t *testing.T) {
	t.Parallel()
	const key = "single-test-bucket"
	keyFunc := func(input string) string {
		return input
	}
	limit1 := NewLimit(rand.Int63n(9)+1, time.Second)
	limit2 := NewLimit(rand.Int63n(99)+1, time.Second)
	limiter := NewLimiter(keyFunc, limit1, limit2)

	now := ntime.Now()

	// any number of peeks should be true
	for range limit1.count * 20 {
		require.True(t, limiter.peek(key, now))
	}

	// no buckets should have been stored
	require.Equal(t, limiter.buckets.count(), 0, "buckets should not persist after peeking")
}

func TestLimiter_Peek_SingleBucket(t *testing.T) {
	t.Parallel()
	const key = "single-test-bucket"
	keyFunc := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyFunc, limit)

	now := ntime.Now()

	// any number of peeks should be true
	for range limit.count * 2 {
		require.True(t, limiter.peek(key, now))
	}

	// exhaust the bucket
	for range limit.count {
		require.True(t, limiter.allow(key, now))
	}

	require.False(t, limiter.peek(key, now))

	// A token is ~111ms
	now = now.Add(limit.durationPerToken)

	require.True(t, limiter.peek(key, now))
	require.True(t, limiter.allow(key, now))
}

func TestLimiter_PeekN_SingleBucket(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyFunc, limit)

	now := ntime.Now()
	require.False(t, limiter.peekN("test", now, 10))
	require.True(t, limiter.peekN("test", now, 9))
	require.True(t, limiter.peekN("test", now, 1))
	require.True(t, limiter.peekN("test", now, 8))

	// exhaust the bucket
	require.True(t, limiter.allowN("test", now, 9))

	require.False(t, limiter.peekN("test", now, 1))

	now = now.Add(3 * limit.durationPerToken)
	require.False(t, limiter.peekN("test", now, 4))
	require.True(t, limiter.peekN("test", now, 3))
	require.True(t, limiter.peekN("test", now, 1))
}

func TestLimiter_Peek_MultipleBuckets(t *testing.T) {
	t.Parallel()
	keyFunc := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyFunc, limit)
	now := ntime.Now()

	// any number of peeks should be true
	for bucketID := range buckets {
		for range limit.count * 2 {
			actual := limiter.peek(bucketID, now)
			require.True(t, actual, now)
		}
	}

	// exhaust all buckets
	for bucketID := range buckets {
		for range limit.count {
			actual := limiter.allow(bucketID, now)
			require.True(t, actual, "bucket %d should have tokens for allow", bucketID)
		}
	}

	for bucketID := range buckets {
		require.False(t, limiter.peek(bucketID, now), "bucket %d should be exhausted for peek", bucketID)
	}

	// A token is ~111ms, refill
	now = now.Add(limit.durationPerToken)

	for bucketID := range buckets {
		require.True(t, limiter.peek(bucketID, now), "bucket %d should have tokens for peek", bucketID)
		require.True(t, limiter.allow(bucketID, now), "bucket %d should have tokens for allow", bucketID)
	}
}

func TestLimiter_Peek_MultipleBuckets_MultipleLimits(t *testing.T) {
	t.Parallel()
	keyFunc := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	perSecond := NewLimit(2, time.Second)
	perMinute := NewLimit(3, time.Minute)
	limiter := NewLimiter(keyFunc, perSecond, perMinute)
	now := ntime.Now()

	// any number of peeks should be true
	for bucketID := range buckets {
		for range 20 {
			actual := limiter.peek(bucketID, now)
			require.True(t, actual, now)
		}
	}

	// exhaust the per-second limit
	for bucketID := range buckets {
		for range perSecond.Count() {
			actual := limiter.allow(bucketID, now)
			require.True(t, actual, "bucket %d should have tokens for allow", bucketID)
		}
	}
	require.False(t, limiter.allow(0, now), "bucket 0 should be exhausted for allow after per-second limit")

	// no peeks should succeed, one of the limits is exceeded
	for bucketID := range buckets {
		for range 20 {
			require.False(t, limiter.peek(bucketID, now), "bucket %d should be exhausted for peek", bucketID)
		}
	}

	// refill both buckets
	now = now.Add(time.Minute)

	for bucketID := range buckets {
		require.True(t, limiter.peek(bucketID, now), "bucket %d should have tokens for peek", bucketID)
		require.True(t, limiter.allow(bucketID, now), "bucket %d should have tokens for allow", bucketID)
	}
}

func TestLimiter_Peek_MultipleBuckets_SingleLimit_Concurrent(t *testing.T) {
	t.Parallel()
	keyFunc := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyFunc, limit)
	now := ntime.Now()

	// Concurrent peeks: all should be true initially
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limit.count * 2 {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					actual := limiter.peek(bucketID, now)
					require.True(t, actual, "concurrent peek should be true for bucket %d (goroutine %d)", bucketID, processID)
				}(bucketID, i)
			}
		}
		wg.Wait()
	}

	// Exhaust all buckets
	for bucketID := range buckets {
		for range limit.count {
			actual := limiter.allow(bucketID, now)
			require.True(t, actual, "bucket %d should have tokens for allow", bucketID)
		}
	}

	// Concurrent peeks: all should be false after exhaustion
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limit.count * 2 {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					actual := limiter.peek(bucketID, now)
					require.False(t, actual, "concurrent peek should be false for exhausted bucket %d (goroutine %d)", bucketID, processID)
				}(bucketID, i)
			}
		}
		wg.Wait()
	}

	// Refill: advance time and test concurrent peeks again, and throw in some allow
	{
		now = now.Add(limit.period)
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limit.count {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					peek := limiter.peek(bucketID, now)
					require.True(t, peek, "concurrent peek should be true for refilled bucket %d (goroutine %d)", bucketID, i)
					allow := limiter.allow(bucketID, now)
					require.True(t, allow, "concurrent allow should be true for refilled bucket %d (goroutine %d)", bucketID, i)
				}(bucketID, i)
			}
		}
		wg.Wait()
	}
}

func TestLimiter_PeekWithDebug(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input
	}
	perSecond := NewLimit(rand.Int63n(9)+1, time.Second)
	perMinute := NewLimit(rand.Int63n(99)+1, time.Minute)
	limiter := NewLimiter(keyFunc, perSecond, perMinute)

	executionTime := ntime.Now()
	input := "test-debug-peek"

	// any number of peeks should be true
	for range 101 {
		allowed, debugs := limiter.peekWithDebug(input, executionTime)
		require.True(t, allowed)
		require.Len(t, debugs, 2, "should have debugs for both limits")

		// Per-second limit debug checks
		d0 := debugs[0]
		require.Equal(t, perSecond, d0.Limit(), "should have per-second limit in details")
		require.Equal(t, allowed, d0.Allowed(), "allowed should match for per-second limit")
		require.Equal(t, executionTime.ToSystemTime(), d0.ExecutionTime(), "execution time should match for per-second limit")
		require.Equal(t, input, d0.Input(), "input should match for per-second limit")
		require.Equal(t, input, d0.Key(), "bucket key should match for per-second limit")
		require.Equal(t, perSecond.Count(), d0.TokensRemaining(), "remaining tokens should match for per-second limit")
		require.Equal(t, int64(1), d0.TokensRequested(), "tokens requested should be 1 for per-second limit")
		require.Equal(t, int64(0), d0.TokensConsumed(), "tokens consumed should be 0 for peek operation on per-second limit")
		require.Equal(t, time.Duration(0), d0.RetryAfter(), "retry after should be 0 for allowed request on per-second limit")

		// Per-minute limit debug checks
		d1 := debugs[1]
		require.Equal(t, perMinute, d1.Limit(), "should have per-minute limit in details")
		require.Equal(t, allowed, d1.Allowed(), "allowed should match for per-minute limit")
		require.Equal(t, executionTime.ToSystemTime(), d1.ExecutionTime(), "execution time should match for per-minute limit")
		require.Equal(t, input, d1.Input(), "input should match for per-minute limit")
		require.Equal(t, input, d1.Key(), "bucket key should match for per-minute limit")
		require.Equal(t, perMinute.Count(), d1.TokensRemaining(), "remaining tokens should match for per-minute limit")
		require.Equal(t, int64(1), d1.TokensRequested(), "tokens requested should be 1 for per-minute limit")
		require.Equal(t, int64(0), d1.TokensConsumed(), "tokens consumed should be 0 for peek operation on per-minute limit")
		require.Equal(t, time.Duration(0), d1.RetryAfter(), "retry after should be 0 for allowed request on per-minute limit")
	}
}

func TestLimiter_PeekWithDebug_AllowedAndDenied(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input
	}
	// Create a small limit to easily test denial scenarios
	limit := NewLimit(2, time.Second)
	limiter := NewLimiter(keyFunc, limit)

	now := ntime.Now()
	input := "test-peek-debug"

	{
		// Test allowed scenario - first peek should be allowed
		allowed, debugs := limiter.peekWithDebug(input, now)
		require.True(t, allowed, "first peek should be allowed")
		require.Len(t, debugs, 1, "should have debug info for single limit")

		d := debugs[0]
		require.True(t, d.Allowed(), "debug should show allowed")
		require.Equal(t, limit, d.Limit(), "should have correct limit")
		require.Equal(t, now.ToSystemTime(), d.ExecutionTime(), "execution time should match")
		require.Equal(t, input, d.Input(), "input should match")
		require.Equal(t, input, d.Key(), "bucket key should match")
		require.Equal(t, limit.count, d.TokensRemaining(), "should have full tokens remaining for peek")
		require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(0), d.TokensConsumed(), "should consume 0 tokens for peek")
		require.Equal(t, time.Duration(0), d.RetryAfter(), "retry after should be 0 for allowed request")
	}

	// Consume all tokens with allow calls to create a denied scenario
	for range limit.count {
		limiter.allow(input, now)
	}

	{
		// Test allowed scenario - peek should now be allowed
		allowed, debugs := limiter.peekWithDebug(input, now)
		require.False(t, allowed, "peek should be denied after tokens exhausted")
		require.Len(t, debugs, 1, "should have debug info for single limit")

		d := debugs[0]
		require.False(t, d.Allowed(), "debug should show denied")
		require.Equal(t, limit, d.Limit(), "should have correct limit")
		require.Equal(t, now.ToSystemTime(), d.ExecutionTime(), "execution time should match")
		require.Equal(t, input, d.Input(), "input should match")
		require.Equal(t, input, d.Key(), "bucket key should match")
		require.Equal(t, int64(0), d.TokensRemaining(), "should have 0 tokens remaining")
		require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(0), d.TokensConsumed(), "should consume 0 tokens for peek even when denied")
		// wait should be for one token
		require.Equal(t, limit.durationPerToken, d.RetryAfter(), "retry after should be duration per token for denied request")
	}
}

func TestLimiter_Peek_SingleBucket_Func(t *testing.T) {
	t.Parallel()
	const key = "single-test-bucket"
	keyFunc := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limitFunc := func(input string) Limit { return limit }
	limiter := NewLimiterFunc(keyFunc, limitFunc)

	now := ntime.Now()

	// any number of peeks should be true
	for range limit.count * 2 {
		require.True(t, limiter.peek(key, now))
	}

	// exhaust the bucket
	for range limit.count {
		require.True(t, limiter.allow(key, now))
	}

	require.False(t, limiter.peek(key, now))

	// A token is ~111ms
	now = now.Add(limit.durationPerToken)

	require.True(t, limiter.peek(key, now))
	require.True(t, limiter.allow(key, now))
}

func TestLimiter_Peek_MultipleBuckets_Func(t *testing.T) {
	t.Parallel()
	keyFunc := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limitFunc := func(input int) Limit { return limit }
	limiter := NewLimiterFunc(keyFunc, limitFunc)
	now := ntime.Now()

	// any number of peeks should be true
	for bucketID := range buckets {
		for range limit.count * 2 {
			actual := limiter.peek(bucketID, now)
			require.True(t, actual, now)
		}
	}

	// exhaust all buckets
	for bucketID := range buckets {
		for range limit.count {
			actual := limiter.allow(bucketID, now)
			require.True(t, actual, "bucket %d should have tokens for allow", bucketID)
		}
	}

	for bucketID := range buckets {
		require.False(t, limiter.peek(bucketID, now), "bucket %d should be exhausted for peek", bucketID)
	}

	// A token is ~111ms, refill
	now = now.Add(limit.durationPerToken)

	for bucketID := range buckets {
		require.True(t, limiter.peek(bucketID, now), "bucket %d should have tokens for peek", bucketID)
		require.True(t, limiter.allow(bucketID, now), "bucket %d should have tokens for allow", bucketID)
	}
}

func TestLimiter_Peek_MultipleBuckets_Concurrent_Func(t *testing.T) {
	t.Parallel()
	keyFunc := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limitFunc := func(input int) Limit { return limit }
	limiter := NewLimiterFunc(keyFunc, limitFunc)
	now := ntime.Now()

	// Concurrent peeks: all should be true initially
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limit.count * 2 {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					actual := limiter.peek(bucketID, now)
					require.True(t, actual, "concurrent peek should be true for bucket %d (goroutine %d)", bucketID, processID)
				}(bucketID, i)
			}
		}
		wg.Wait()
	}

	// Exhaust all buckets
	for bucketID := range buckets {
		for range limit.count {
			actual := limiter.allow(bucketID, now)
			require.True(t, actual, "bucket %d should have tokens for allow", bucketID)
		}
	}

	// Concurrent peeks: all should be false after exhaustion
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limit.count * 2 {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					actual := limiter.peek(bucketID, now)
					require.False(t, actual, "concurrent peek should be false for exhausted bucket %d (goroutine %d)", bucketID, processID)
				}(bucketID, i)
			}
		}
		wg.Wait()
	}

	// Refill: advance time and test concurrent peeks again, and throw in some allow
	{
		now = now.Add(limit.period)
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limit.count {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					peek := limiter.peek(bucketID, now)
					require.True(t, peek, "concurrent peek should be true for refilled bucket %d (goroutine %d)", bucketID, i)
					allow := limiter.allow(bucketID, now)
					require.True(t, allow, "concurrent allow should be true for refilled bucket %d (goroutine %d)", bucketID, i)
				}(bucketID, i)
			}
		}
		wg.Wait()
	}
}

func TestLimiter_PeekNWithDebug_SingleBucket(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input + "-key"
	}
	perSecond := NewLimit(9, time.Second)
	perHour := NewLimit(99, time.Hour)
	limiter := NewLimiter(keyFunc, perSecond, perHour)

	now := ntime.Now()

	{
		request := int64(10)
		allowed, debugs := limiter.peekNWithDebug("test-allow-with-debug", now, request)
		require.False(t, allowed)
		require.Len(t, debugs, 2, "should have debugs for both limits")

		d0 := debugs[0]
		require.False(t, d0.Allowed())
		require.Equal(t, d0.Input(), "test-allow-with-debug")
		require.Equal(t, d0.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d0.ExecutionTime(), now.ToSystemTime())
		require.Equal(t, d0.TokensRequested(), request)
		require.Equal(t, d0.TokensConsumed(), int64(0))
		require.Equal(t, d0.TokensRemaining(), perSecond.count)

		d1 := debugs[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.Input(), "test-allow-with-debug")
		require.Equal(t, d1.Key(), "test-allow-with-debug-key")
		require.Equal(t, d1.ExecutionTime(), now.ToSystemTime())
		require.Equal(t, d1.TokensRequested(), request)
		require.Equal(t, d1.TokensConsumed(), int64(0))
		require.Equal(t, d1.TokensRemaining(), perHour.count)
	}

	{
		request := int64(9)
		allowed, debugs := limiter.peekNWithDebug("test-allow-with-debug", now, request)
		require.True(t, allowed)
		require.Len(t, debugs, 2, "should have debugs for both limits")

		d0 := debugs[0]
		require.True(t, d0.Allowed())
		require.Equal(t, d0.Input(), "test-allow-with-debug")
		require.Equal(t, d0.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d0.ExecutionTime(), now.ToSystemTime())
		require.Equal(t, d0.TokensRequested(), request)
		require.Equal(t, d0.TokensConsumed(), int64(0))
		require.Equal(t, d0.TokensRemaining(), perSecond.count)

		d1 := debugs[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.Input(), "test-allow-with-debug")
		require.Equal(t, d1.Key(), "test-allow-with-debug-key")
		require.Equal(t, d1.ExecutionTime(), now.ToSystemTime())
		require.Equal(t, d1.TokensRequested(), request)
		require.Equal(t, d1.TokensConsumed(), int64(0))
		require.Equal(t, d1.TokensRemaining(), perHour.count)
	}
}

func TestLimiter_PeekWithDebug_Func(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limitFunc := func(input string) Limit { return limit }
	limiter := NewLimiterFunc(keyFunc, limitFunc)

	now := ntime.Now()
	input := "test-details"

	allowed, debugs := limiter.peekWithDebug(input, now)
	require.True(t, allowed)
	require.Len(t, debugs, 1, "should have debug info for single limit")

	d := debugs[0]
	require.Equal(t, limit, d.Limit(), "should have correct limit")
	require.True(t, d.Allowed(), "should be allowed")
	require.Equal(t, now.ToSystemTime(), d.ExecutionTime(), "execution time should match")
	require.Equal(t, input, d.Input(), "input should match")
	require.Equal(t, input, d.Key(), "bucket key should match")
	require.Equal(t, limit.count, d.TokensRemaining(), "remaining tokens should match")
	require.Equal(t, int64(1), d.TokensRequested(), "tokens requested should be 1")
	require.Equal(t, int64(0), d.TokensConsumed(), "tokens consumed should be 0 for peek operation")
	require.Equal(t, time.Duration(0), d.RetryAfter(), "retry after should be 0 for allowed request")
}

func TestLimiter_PeekWithDetails(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string { return input }

	// Test single limit
	t.Run("SingleLimit", func(t *testing.T) {
		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)

		// Test when peek shows available tokens
		allowed, details := limiter.PeekWithDetails("test-key1")
		require.True(t, allowed, "peek should show available tokens")
		require.Equal(t, true, details.Allowed(), "should be allowed")
		require.Equal(t, int64(1), details.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(0), details.TokensConsumed(), "peek should never consume tokens")
		require.Equal(t, int64(5), details.TokensRemaining(), "should have all 5 tokens remaining")
		require.Equal(t, time.Duration(0), details.RetryAfter(), "retry after should be 0 when available")
	})

	// Test RetryAfter with multiple limits - the key scenario
	t.Run("MultipleLimits_RetryAfter", func(t *testing.T) {
		// Create limits with different refill rates to test RetryAfter logic
		fast := NewLimit(10, time.Second) // 10 per second = 100ms per token
		slow := NewLimit(6, time.Minute)  // 6 per minute = 10s per token
		limiter := NewLimiter(keyFunc, fast, slow)

		baseTime := ntime.Now()

		// Test when both buckets are exhausted - RetryAfter should be max across buckets
		t.Run("BothBucketsExhausted", func(t *testing.T) {
			// Exhaust both buckets by consuming tokens first
			for i := 0; i < 6; i++ { // Can only consume 6 due to slow bucket limit
				allowed := limiter.allowN("test-peek-exhausted", baseTime, 1)
				require.True(t, allowed, "request %d should be allowed", i)
			}

			// Now peek should show both buckets exhausted
			allowed, details := limiter.peekNWithDetails("test-peek-exhausted", baseTime, 1)
			require.False(t, allowed, "peek should show no tokens available when exhausted")
			require.Equal(t, int64(0), details.TokensConsumed(), "peek never consumes")

			// RetryAfter should be the MAX time needed across buckets
			// Fast bucket needs 100ms for next token, slow bucket needs 10s
			// So RetryAfter should be ~10s (the slower one)
			expectedSlowRetry := slow.DurationPerToken() // 10s
			require.GreaterOrEqual(t, details.RetryAfter(), expectedSlowRetry-time.Millisecond,
				"retry after should be at least slow bucket duration")
			require.LessOrEqual(t, details.RetryAfter(), expectedSlowRetry+time.Millisecond,
				"retry after should not exceed slow bucket duration by much")
		})

		// Test requesting multiple tokens - RetryAfter should account for N tokens
		t.Run("MultipleTokensRequest", func(t *testing.T) {
			// Consume some tokens to create the right scenario
			for i := 0; i < 3; i++ { // Use 3 tokens, leaving fast=7, slow=3
				allowed := limiter.allowN("test-peek-multi-tokens", baseTime, 1)
				require.True(t, allowed, "request %d should be allowed", i)
			}

			// Peek for 4 tokens - should be denied because slow bucket only has 3
			allowed, details := limiter.peekNWithDetails("test-peek-multi-tokens", baseTime, 4)
			require.False(t, allowed, "peek for 4 tokens should show not available")
			require.Equal(t, int64(4), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed(), "peek never consumes")

			// RetryAfter should be time needed for slow bucket to accumulate 4 tokens
			// Since slow bucket has 3 tokens, it needs 1 more token = 10s
			expectedRetry := slow.DurationPerToken() * 1 // 10s for 1 additional token
			require.GreaterOrEqual(t, details.RetryAfter(), expectedRetry-time.Millisecond,
				"retry after should account for tokens needed in limiting bucket")
			require.LessOrEqual(t, details.RetryAfter(), expectedRetry+time.Millisecond,
				"retry after should not exceed expected duration by much")
		})

		// Test when peek shows available - RetryAfter should be 0
		t.Run("AvailableTokens", func(t *testing.T) {
			// Fresh limiter, peek should show available
			freshLimiter := NewLimiter(keyFunc, fast, slow)
			allowed, details := freshLimiter.peekNWithDetails("test-peek-available", baseTime, 1)
			require.True(t, allowed, "peek should show available on fresh limiter")
			require.Equal(t, time.Duration(0), details.RetryAfter(),
				"retry after should be 0 when peek shows available")
		})

		// Test peek vs allow RetryAfter consistency
		t.Run("PeekAllowRetryAfterConsistency", func(t *testing.T) {
			// Consume tokens to exhaust slow bucket
			for i := 0; i < 6; i++ {
				allowed := limiter.allowN("test-consistency", baseTime, 1)
				require.True(t, allowed, "setup request %d should be allowed", i)
			}

			// Both peek and allow should show same RetryAfter when denied
			peekAllowed, peekDetails := limiter.peekNWithDetails("test-consistency", baseTime, 1)
			allowAllowed, allowDetails := limiter.allowNWithDetails("test-consistency", baseTime, 1)

			require.False(t, peekAllowed, "peek should show not available")
			require.False(t, allowAllowed, "allow should show not available")

			// CRITICAL: RetryAfter should be identical between peek and allow
			require.Equal(t, peekDetails.RetryAfter(), allowDetails.RetryAfter(),
				"peek and allow should calculate identical RetryAfter times")
		})
	})

	// Test edge cases
	t.Run("EdgeCases", func(t *testing.T) {
		// Test no limits defined
		t.Run("NoLimits", func(t *testing.T) {
			limiter := NewLimiter(keyFunc) // No limits
			allowed, details := limiter.PeekWithDetails("test-no-limits")
			require.True(t, allowed, "should show available when no limits defined")
			require.Equal(t, int64(1), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed(), "peek never consumes")
			require.Equal(t, int64(0), details.TokensRemaining(), "should show 0 remaining when no limits")
			require.Equal(t, time.Duration(0), details.RetryAfter(), "should show 0 retry after when no limits")
		})

		// Test requesting zero tokens
		t.Run("ZeroTokensRequest", func(t *testing.T) {
			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			allowed, details := limiter.PeekNWithDetails("test-zero", 0)
			require.True(t, allowed, "peeking 0 tokens should always show available")
			require.Equal(t, int64(0), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(5), details.TokensRemaining(), "should show all tokens remaining")
			require.Equal(t, time.Duration(0), details.RetryAfter())
		})

		// Test peek doesn't mutate bucket state
		t.Run("PeekDoesNotMutate", func(t *testing.T) {
			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)

			// Multiple peeks should show same state
			for i := 0; i < 5; i++ {
				allowed, details := limiter.PeekWithDetails("test-no-mutate")
				require.True(t, allowed, "peek %d should show available", i)
				require.Equal(t, int64(5), details.TokensRemaining(),
					"remaining tokens should not change after peek %d", i)
			}

			// After all those peeks, allow should still work and show same initial state
			allowed, details := limiter.AllowWithDetails("test-no-mutate")
			require.True(t, allowed, "allow should still work after multiple peeks")
			require.Equal(t, int64(4), details.TokensRemaining(), "should have 4 tokens after consuming 1")
		})
	})
}
