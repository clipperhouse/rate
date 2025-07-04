package rate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiter_Allow_SingleBucket(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()

	for range limit.count {
		require.True(t, limiter.allow("test", now))
	}

	require.False(t, limiter.allow("test", now))

	// A token is ~111ms
	now = now.Add(limit.durationPerToken)

	require.True(t, limiter.allow("test", now))
}

func TestLimiter_Allow_MultipleBuckets(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
	now := time.Now()

	for i := range buckets {
		for range 9 {
			limiter.allow(fmt.Sprintf("test-%d", i), now)
		}
		require.False(t, limiter.allow(fmt.Sprintf("test-%d", i), now))
	}
}

func TestLimiter_Allow_MultipleBuckets_Concurrent(t *testing.T) {
	keyer := func(bucketID int) string {
		return fmt.Sprintf("test-bucket-%d", bucketID)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
	start := time.Now()

	var wg sync.WaitGroup

	// Enough concurrent processes for each bucket to precisely exhaust the limit
	for bucketID := range buckets {
		for processID := range limit.count {
			wg.Add(1)
			go func(bucketID int, processID int64) {
				defer wg.Done()
				allowed := limiter.allow(bucketID, start)
				require.True(t, allowed, "process %d for bucket %s should be allowed", processID, keyer(bucketID))
			}(bucketID, processID)
		}
	}

	wg.Wait()

	// Verify that additional requests are rejected, all buckets should be exhausted
	for bucketID := range buckets {
		allowed := limiter.allow(bucketID, start)
		require.False(t, allowed, "bucket %d should be exhausted after %d requests", bucketID, limit.count)
	}

	// Complete refill
	now := start.Add(limit.period)
	for bucketID := range buckets {
		for range limit.count {
			allowed := limiter.allow(bucketID, now)
			require.True(t, allowed, "bucket %d should be refilled after 1 second", bucketID)
		}
	}
}

func TestLimiter_AllowWithDetails(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()

	allow, details := limiter.allowWithDetails("test-details", now)
	require.True(t, allow)
	require.Equal(t, limit, details.Limit())
	require.Equal(t, now, details.ExecutionTime())
	require.Equal(t, "test-details", details.BucketKey())
	require.Equal(t, limit.count-1, details.RemainingTokens())
}

func TestLimiter_Peek_SingleBucket(t *testing.T) {
	const key = "single-test-bucket"
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()

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

func TestLimiter_Peek_MultipleBuckets(t *testing.T) {
	keyer := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
	now := time.Now()

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

func TestLimiter_Peek_MultipleBuckets_Concurrent(t *testing.T) {
	keyer := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
	now := time.Now()

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

func TestLimiter_PeekWithDetails(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()

	allowed, details := limiter.peekWithDetails("test-details", now)
	require.True(t, allowed)
	require.Equal(t, limit, details.Limit())
	require.Equal(t, now, details.ExecutionTime())
	require.Equal(t, "test-details", details.BucketKey())
	require.Equal(t, limit.count, details.RemainingTokens())
}

func TestLimiter_Allow_SingleBucket_Func(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limitFunc := func(input string) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)

	now := time.Now()

	for range limit.count {
		require.True(t, limiter.allow("test", now))
	}

	require.False(t, limiter.allow("test", now))

	// A token is ~111ms
	now = now.Add(limit.durationPerToken)

	require.True(t, limiter.allow("test", now))
}

func TestLimiter_Allow_MultipleBuckets_Func(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limitFunc := func(input string) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)
	now := time.Now()

	for i := range buckets {
		for range 9 {
			limiter.allow(fmt.Sprintf("test-%d", i), now)
		}
		require.False(t, limiter.allow(fmt.Sprintf("test-%d", i), now))
	}
}

func TestLimiter_Allow_MultipleBuckets_Concurrent_Func(t *testing.T) {
	keyer := func(bucketID int) string {
		return fmt.Sprintf("test-bucket-%d", bucketID)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limitFunc := func(input int) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)
	start := time.Now()

	var wg sync.WaitGroup

	// Enough concurrent processes for each bucket to precisely exhaust the limit
	for bucketID := range buckets {
		for processID := range limit.count {
			wg.Add(1)
			go func(bucketID int, processID int64) {
				defer wg.Done()
				allowed := limiter.allow(bucketID, start)
				require.True(t, allowed, "process %d for bucket %s should be allowed", processID, keyer(bucketID))
			}(bucketID, processID)
		}
	}

	wg.Wait()

	// Verify that additional requests are rejected, all buckets should be exhausted
	for bucketID := range buckets {
		allowed := limiter.allow(bucketID, start)
		require.False(t, allowed, "bucket %d should be exhausted after %d requests", bucketID, limit.count)
	}

	// Complete refill
	now := start.Add(limit.period)
	for bucketID := range buckets {
		for range limit.count {
			allowed := limiter.allow(bucketID, now)
			require.True(t, allowed, "bucket %d should be refilled after 1 second", bucketID)
		}
	}
}

func TestLimiter_AllowWithDetails_Func(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(11, time.Second)
	limitFunc := func(input string) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)

	now := time.Now()

	allow, details := limiter.allowWithDetails("test-details", now)
	require.True(t, allow)
	require.Equal(t, limit, details.Limit())
	require.Equal(t, now, details.ExecutionTime())
	require.Equal(t, "test-details", details.BucketKey())
	require.Equal(t, limit.count-1, details.RemainingTokens())
}

func TestLimiter_Peek_SingleBucket_Func(t *testing.T) {
	const key = "single-test-bucket"
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limitFunc := func(input string) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)

	now := time.Now()

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
	keyer := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limitFunc := func(input int) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)
	now := time.Now()

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
	keyer := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limitFunc := func(input int) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)
	now := time.Now()

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

func TestLimiter_PeekWithDetails_Func(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limitFunc := func(input string) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)

	now := time.Now()

	allowed, details := limiter.peekWithDetails("test-details", now)
	require.True(t, allowed)
	require.Equal(t, limit, details.Limit())
	require.Equal(t, now, details.ExecutionTime())
	require.Equal(t, "test-details", details.BucketKey())
	require.Equal(t, limit.count, details.RemainingTokens())
}

func TestLimiter_UsesLimitFunc(t *testing.T) {
	keyer := func(input int) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	{
		limitFunc := func(input int) Limit {
			return NewLimit(int64(10*input), time.Second)
		}
		limiter := NewLimiterFunc(keyer, limitFunc)

		for i := range 3 {
			allow, details := limiter.allowWithDetails(i+1, time.Now())
			require.True(t, allow)
			require.Equal(t, limitFunc(i+1), details.Limit())
		}
	}
	{
		limit := NewLimit(888, time.Second)
		limiter := NewLimiter(keyer, limit)

		for i := range 3 {
			allow, details := limiter.allowWithDetails(i+1, time.Now())
			require.True(t, allow)
			require.Equal(t, limit, details.Limit())
		}
	}
}

func TestLimiter_Wait_SingleBucket(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Consume all tokens
	for range limit.count {
		ok := limiter.allow("test", executionTime)
		require.True(t, ok, "should allow initial tokens")
	}

	// Should not allow immediately
	ok := limiter.allow("test", executionTime)
	require.False(t, ok, "should not allow when tokens exhausted")

	// Wait for a token, with sufficient timeout
	{
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(limit.durationPerToken, cancel)
		allow := limiter.wait(ctx, "test", executionTime)
		require.True(t, allow, "should acquire token after waiting")

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)
	}

	// Should not allow again immediately after wait
	ok = limiter.allow("test", executionTime)
	require.False(t, ok, "should not allow again immediately after wait")

	// Wait with a context that times out before token is available
	{
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(limit.durationPerToken/5, cancel)
		allow := limiter.wait(ctx, "test", executionTime)
		require.False(t, allow, "should not acquire token if context times out first")
	}
}

func TestLimiter_Wait_MultipleBuckets(t *testing.T) {
	keyer := func(input int) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	const buckets = 3
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)
	executionTime := time.Now()

	// Exhaust tokens for all buckets
	for bucketID := range buckets {
		for range limit.count {
			allow := limiter.allow(bucketID, executionTime)
			require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
		}
	}

	// Should not allow immediately for any bucket
	for bucketID := range buckets {
		allow := limiter.allow(bucketID, executionTime)
		require.False(t, allow, "should not allow when tokens exhausted for bucket %d", bucketID)
	}

	// Wait for a token for each bucket
	for bucketID := range buckets {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(limit.durationPerToken, cancel)
		allow := limiter.wait(ctx, bucketID, executionTime)
		require.True(t, allow, "should acquire token after waiting for bucket %d", bucketID)
	}

	// We waited
	executionTime = executionTime.Add(limit.durationPerToken)

	// Buckets should be empty again
	for bucketID := range buckets {
		ok := limiter.allow(bucketID, executionTime)
		require.False(t, ok, "should not allow again immediately after wait for bucket %d", bucketID)
	}
}

func TestLimiter_Wait_MultipleBuckets_Concurrent(t *testing.T) {
	keyer := func(input int) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	const buckets = 3
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)
	executionTime := time.Now()

	// Exhaust tokens for all buckets
	for bucketID := range buckets {
		for range limit.count {
			allow := limiter.allow(bucketID, executionTime)
			require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
		}
	}

	// Buckets should be empty
	for bucketID := range buckets {
		allow := limiter.allow(bucketID, executionTime)
		require.False(t, allow, "should not allow when tokens exhausted for bucket %d", bucketID)
	}

	fudge := 2 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(limit.durationPerToken+fudge, cancel)

	// Concurrently wait for a token for each bucket
	var wg sync.WaitGroup
	wg.Add(buckets)
	for bucketID := range buckets {
		go func(i int) {
			defer wg.Done()
			allow := limiter.wait(ctx, i, executionTime)
			require.True(t, allow, "should acquire token after waiting for bucket %d", i)
		}(bucketID)
	}
	wg.Wait()

	// We waited
	executionTime = executionTime.Add(limit.durationPerToken + fudge)

	// Buckets should be empty again
	for bucketID := range buckets {
		ok := limiter.allow(bucketID, executionTime)
		require.False(t, ok, "should not allow again immediately after wait for bucket %d", bucketID)
	}
}
