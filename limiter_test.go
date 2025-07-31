package rate

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

/*
Trying to cover all the combinatorial use cases:

For each method: Allow, Peek, Wait, Details
- Single & multiple buckets
- Single & multiple limits
- Serial & concurrent

There will certainly be some redundancy — for example the concurrent
tests will be a superset of the serial tests. I prefer having
small, narrow tests to allow fast repros, plus broad tests that
are essentially integrations.
*/

func TestLimiter_Allow_AlwaysPersists(t *testing.T) {
	t.Parallel()
	keyer := func(input int) string {
		return fmt.Sprintf("bucket-allow-always-persists-%d", input)
	}
	limit1 := NewLimit(9, time.Second)
	limit2 := NewLimit(99, time.Second)
	limiter := NewLimiter(keyer, limit1, limit2)
	const buckets = 3

	now := time.Now()

	for bucketID := range buckets {
		limiter.allow(bucketID, now)
	}

	expected := buckets * len(limiter.limits)
	require.Equal(t, limiter.buckets.count(), expected, "buckets should have persisted after allow")
}

func TestLimiter_Allow_SingleBucket(t *testing.T) {
	t.Parallel()
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

func TestLimiter_AllowN_SingleBucket(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()
	require.True(t, limiter.allowN("test", now, 1))
	require.True(t, limiter.allowN("test", now, 8))
	require.False(t, limiter.allowN("test", now, 1))

	now = now.Add(3 * limit.durationPerToken)
	require.True(t, limiter.allowN("test", now, 2))
	require.True(t, limiter.allowN("test", now, 1))
	require.False(t, limiter.allowN("test", now, 1))
}

func TestLimiter_Allow_SingleBucket_MultipleLimits(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}

	// Set up a limiter with multiple limits:
	// - 2 requests per second (more restrictive)
	// - 3 requests per minute (less restrictive per-second, but more restrictive overall)
	// This demonstrates that ALL limits must allow for a token to be consumed
	perSecond := NewLimit(2, time.Second)
	perMinute := NewLimit(3, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)

	now := time.Now()

	// Consume tokens up to the per-second limit
	calls := perSecond.Count() // 2
	for i := range calls {
		allowed := limiter.allow("test", now)
		require.True(t, allowed, "should allow token %d", i+1)
	}

	//...leaving 0 in the per-second bucket and 1 in the per-minute bucket

	// 2 tokens should have been consumed from both buckets
	_, details1 := limiter.peekWithDetails("test", now)
	require.Equal(t, perSecond.Count()-calls, details1[0].TokensRemaining(), "per-second bucket should be exhausted")
	require.Equal(t, perMinute.Count()-calls, details1[1].TokensRemaining(), "per-minute bucket should have 1 token remaining")

	// Per-second limit exhausted, but per-minute has 1 token left,
	// should fail because we require all limits to allow
	allowed := limiter.allow("test", now)
	require.False(t, allowed, "should not allow when per-second limit is exhausted")

	// Verify that no tokens were consumed from any bucket,
	// because the above request was denied
	_, details2 := limiter.peekWithDetails("test", now)
	require.Equal(t, details1, details2, "details should be the same before and after the failed request")

	// Refill per-second bucket (to 2), by forwarding time; per-minute still has 1
	now = now.Add(time.Second)
	// Consume 1 token from both buckets
	allowed = limiter.allow("test", now)
	require.True(t, allowed, "should allow after per-second bucket refills")

	// ...leaving 1 in the per-second bucket, 0 in the per-minute bucket

	// Even though per-second has 1 token, should fail because per-minute is exhausted
	allowed = limiter.allow("test", now)
	require.False(t, allowed, "should not allow when per-minute limit is exhausted")

	// Verify per-second bucket was not affected by the failed request
	_, details3 := limiter.peekWithDetails("test", now)
	require.Equal(t, int64(1), details3[0].TokensRemaining(), "per-second bucket should be unchanged after denial")
	require.Equal(t, int64(0), details3[1].TokensRemaining(), "per-minute bucket should remain exhausted")

	// Refill both buckets by advancing time
	now = now.Add(time.Minute)

	// Should work now that per-minute is refilled
	allowed = limiter.allow("test", now)
	require.True(t, allowed, "should allow after per-minute bucket refills")
}

func TestLimiter_Allow_MultipleBuckets_SingleLimit(t *testing.T) {
	t.Parallel()
	keyer := func(input int) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	const buckets = 3

	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
	now := time.Now()

	for bucketID := range buckets {
		for range 9 {
			require.True(t, limiter.allow(bucketID, now))
		}
		require.False(t, limiter.allow(bucketID, now))
	}
}

func TestLimiter_Allow_MultipleBuckets_MultipleLimits(t *testing.T) {
	t.Parallel()
	keyer := func(input int) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	const buckets = 2
	perSecond := NewLimit(2, time.Second)
	perMinute := NewLimit(3, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)
	executionTime := time.Now()

	for bucketID := range buckets {
		for range perSecond.Count() {
			// exhaust the per-second limit
			require.True(t, limiter.allow(bucketID, executionTime), "bucket %d should allow request", bucketID)
		}
		// per-second limit exhausted, per-minute has 1 token remaining
		require.False(t, limiter.allow(bucketID, executionTime), "bucket %d should not allow request", bucketID)

		// other buckets should be unaffected
		require.True(t, limiter.peek(bucketID+1, executionTime), "bucket %d should allow request", bucketID+1)
		require.True(t, limiter.peek(bucketID+2, executionTime), "bucket %d should allow request", bucketID+2)

		// refill per-second limit
		executionTime = executionTime.Add(time.Second)
		require.True(t, limiter.allow(bucketID, executionTime), "bucket %d should allow request", bucketID)
	}
}

func TestLimiter_Allow_MultipleBuckets_SingleLimit_Concurrent(t *testing.T) {
	t.Parallel()
	keyer := func(bucketID int) string {
		return fmt.Sprintf("test-bucket-%d", bucketID)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
	start := time.Now()

	// Enough concurrent processes for each bucket to precisely exhaust the limit
	{
		var wg sync.WaitGroup
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
	}

	// Verify that additional requests are rejected, all buckets should be exhausted
	for bucketID := range buckets {
		allowed := limiter.allow(bucketID, start)
		require.False(t, allowed, "bucket %d should be exhausted after %d requests", bucketID, limit.count)
	}

	// Complete refill
	now := start.Add(limit.period)
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				for range limit.count {
					allowed := limiter.allow(bucketID, now)
					require.True(t, allowed, "bucket %d should be refilled after 1 second", bucketID)
				}
			}(bucketID)
		}
		wg.Wait()
	}
}

func TestLimiter_Allow_MultipleBuckets_MultipleLimits_Concurrent(t *testing.T) {
	t.Parallel()
	keyer := func(bucketID int) string {
		return fmt.Sprintf("test-bucket-%d", bucketID)
	}
	const buckets = 3
	perSecond := NewLimit(2, time.Second)
	perMinute := NewLimit(3, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)
	executionTime := time.Now()

	// Enough concurrent processes for each bucket to precisely exhaust the per-second limit
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for processID := range perSecond.count {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					allowed := limiter.allow(bucketID, executionTime)
					require.True(t, allowed, "process %d for bucket %s should be allowed", processID, keyer(bucketID))
				}(bucketID, processID)
			}
		}
		wg.Wait()
	}

	// Verify that additional requests are rejected, all buckets should be exhausted on per-second limit
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed := limiter.allow(bucketID, executionTime)
				require.False(t, allowed, "bucket %d should be exhausted after %d requests", bucketID, perSecond.count)
			}(bucketID)
		}
		wg.Wait()
	}

	// Refill per-second limit by advancing time
	executionTime = executionTime.Add(time.Second)

	// Each bucket should now have 1 more token available from per-second limit,
	// but per-minute limit should still have 1 token remaining (3-2=1)
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed := limiter.allow(bucketID, executionTime)
				require.True(t, allowed, "bucket %d should allow request after per-second refill", bucketID)
			}(bucketID)
		}
		wg.Wait()
	}

	// Now all buckets should be exhausted on per-minute limit (used all 3 tokens: 2 initially + 1 after refill)
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed := limiter.allow(bucketID, executionTime)
				require.False(t, allowed, "bucket %d should be exhausted on per-minute limit", bucketID)
			}(bucketID)
		}
		wg.Wait()
	}

	// Complete refill by advancing to refill both limits
	executionTime = executionTime.Add(time.Minute)

	// Test concurrent access after full refill
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for processID := range perSecond.count {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					allowed := limiter.allow(bucketID, executionTime)
					require.True(t, allowed, "process %d for bucket %s should be allowed after refill", processID, keyer(bucketID))
				}(bucketID, processID)
			}
		}
		wg.Wait()
	}
}

func TestLimiter_AllowNWithDetails_SingleBucket(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input + "-key"
	}
	perSecond := NewLimit(9, time.Second)
	perHour := NewLimit(99, time.Hour)
	limiter := NewLimiter(keyer, perSecond, perHour)

	now := time.Now()

	// previously consumed, running tally
	consumed := int64(0)

	{
		consume := int64(3)
		allowed, details := limiter.allowNWithDetails("test-allow-with-details", now, consume)
		require.True(t, allowed)
		require.Len(t, details, 2, "should have details for both limits")

		d0 := details[0]
		require.True(t, d0.Allowed())
		require.Equal(t, d0.BucketInput(), "test-allow-with-details")
		require.Equal(t, d0.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d0.ExecutionTime(), now)
		require.Equal(t, d0.TokensRequested(), consume)
		require.Equal(t, d0.TokensConsumed(), consume)
		require.Equal(t, d0.TokensRemaining(), perSecond.count-consume)

		d1 := details[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.BucketInput(), "test-allow-with-details")
		require.Equal(t, d1.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d1.ExecutionTime(), now)
		require.Equal(t, d1.TokensRequested(), consume)
		require.Equal(t, d1.TokensConsumed(), consume)
		require.Equal(t, d1.TokensRemaining(), perHour.count-consume)

		consumed += consume
	}

	// should still be ok
	{
		consume := int64(6)
		allowed, details := limiter.allowNWithDetails("test-allow-with-details", now, consume)
		require.True(t, allowed)
		require.Len(t, details, 2, "should have details for both limits")

		d0 := details[0]
		require.True(t, d0.Allowed())
		require.Equal(t, d0.BucketInput(), "test-allow-with-details")
		require.Equal(t, d0.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d0.ExecutionTime(), now)
		require.Equal(t, d0.TokensRequested(), consume)
		require.Equal(t, d0.TokensConsumed(), consume)
		require.Equal(t, d0.TokensRemaining(), perSecond.count-consume-consumed)

		d1 := details[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.BucketInput(), "test-allow-with-details")
		require.Equal(t, d1.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d1.ExecutionTime(), now)
		require.Equal(t, d1.TokensRequested(), consume)
		require.Equal(t, d1.TokensConsumed(), consume)
		require.Equal(t, d1.TokensRemaining(), perHour.count-consume-consumed)

		consumed += consume
	}

	// per-second now exhausted, should be denied, no tokens consumed
	{
		consume := int64(2)
		allowed, details := limiter.allowNWithDetails("test-allow-with-details", now, consume)
		require.False(t, allowed)
		require.Len(t, details, 2, "should have details for both limits")

		d0 := details[0]
		require.False(t, d0.Allowed())
		require.Equal(t, d0.BucketInput(), "test-allow-with-details")
		require.Equal(t, d0.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d0.ExecutionTime(), now)
		require.Equal(t, d0.TokensRequested(), consume)
		require.Equal(t, d0.TokensConsumed(), int64(0))
		require.Equal(t, d0.TokensRemaining(), perSecond.count-consumed)

		d1 := details[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.BucketInput(), "test-allow-with-details")
		require.Equal(t, d1.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d1.ExecutionTime(), now)
		require.Equal(t, d1.TokensRequested(), consume)
		require.Equal(t, d1.TokensConsumed(), int64(0))
		require.Equal(t, d1.TokensRemaining(), perHour.count-consumed)

		consumed += 0
	}

	refilled := int64(3)
	now = now.Add(time.Duration(refilled) * perSecond.durationPerToken)

	// per-second refilled
	{
		consume := int64(2)
		allowed, details := limiter.allowNWithDetails("test-allow-with-details", now, consume)
		require.True(t, allowed)
		require.Len(t, details, 2, "should have details for both limits")

		d0 := details[0]
		require.True(t, d0.Allowed())
		require.Equal(t, d0.BucketInput(), "test-allow-with-details")
		require.Equal(t, d0.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d0.ExecutionTime(), now)
		require.Equal(t, d0.TokensRequested(), consume)
		require.Equal(t, d0.TokensConsumed(), consume)
		require.Equal(t, d0.TokensRemaining(), perSecond.count-consume-consumed+refilled)

		d1 := details[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.BucketInput(), "test-allow-with-details")
		require.Equal(t, d1.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d1.ExecutionTime(), now)
		require.Equal(t, d1.TokensRequested(), consume)
		require.Equal(t, d1.TokensConsumed(), consume)
		require.Equal(t, d1.TokensRemaining(), perHour.count-consume-consumed+0) // time passing not enough to refill per-hour
	}
}

func TestLimiter_AllowWithDetails_SingleBucket_MultipleLimits(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	perSecond := NewLimit(2, time.Second)
	perMinute := NewLimit(3, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)

	executionTime := time.Now()
	{
		// exhaust the per-second limit, but per-minute should still have 1 token
		for i := range perSecond.Count() {
			allowed, details := limiter.allowWithDetails("test-allow-with-details", executionTime)
			require.True(t, allowed)
			require.Len(t, details, 2, "should have details for both limits")

			require.Equal(t, perSecond, details[0].Limit(), "should have per-second limit in details")
			require.Equal(t, allowed, details[0].Allowed(), "allowed should match for per-second limit")
			require.Equal(t, executionTime, details[0].ExecutionTime(), "execution time should match for per-second limit")
			require.Equal(t, "test-allow-with-details", details[0].BucketKey(), "bucket key should match for per-second limit")
			require.Equal(t, perSecond.Count()-i-1, details[0].TokensRemaining(), "remaining tokens should match for per-second limit")

			require.Equal(t, perMinute, details[1].Limit(), "should have per-minute limit in details")
			require.Equal(t, allowed, details[1].Allowed(), "allowed should match for per-minute limit")
			require.Equal(t, executionTime, details[1].ExecutionTime(), "execution time should match for per-minute limit")
			require.Equal(t, "test-allow-with-details", details[1].BucketKey(), "bucket key should match for per-minute limit")
			require.Equal(t, perMinute.Count()-i-1, details[1].TokensRemaining(), "remaining tokens should match for per-minute limit")
		}
	}

	{
		allowed, details := limiter.allowWithDetails("test-allow-with-details", executionTime)
		require.False(t, allowed)
		require.Len(t, details, 2, "should have details for both limits")

		// per-second should have been denied
		require.False(t, details[0].Allowed(), "allowed should match for per-second limit")
		require.Equal(t, int64(0), details[0].TokensRemaining(), "remaining tokens should match for per-second limit")
		require.Equal(t, perSecond, details[0].Limit(), "should have per-second limit in details")
		require.Equal(t, executionTime, details[0].ExecutionTime(), "execution time should match for per-second limit")
		require.Equal(t, "test-allow-with-details", details[0].BucketKey(), "bucket key should match for per-second limit")

		// per-minute still has 1 token
		require.True(t, details[1].Allowed(), "allowed should match for per-minute limit")
		require.Equal(t, int64(1), details[1].TokensRemaining(), "per-minute limit should have 1 remaining token")
		require.Equal(t, perMinute, details[1].Limit(), "should have per-minute limit in details")
		require.Equal(t, executionTime, details[1].ExecutionTime(), "execution time should match for per-minute limit")
		require.Equal(t, "test-allow-with-details", details[1].BucketKey(), "bucket key should match for per-minute limit")
	}
}

func TestLimiter_AllowWithDetails_MultipleBuckets_MultipleLimits_Concurrent(t *testing.T) {
	t.Parallel()
	keyer := func(bucketID int) string {
		return fmt.Sprintf("test-bucket-%d", bucketID)
	}
	const buckets = 3
	perSecond := NewLimit(2, time.Second)
	perMinute := NewLimit(3, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)
	executionTime := time.Now()

	// Enough concurrent processes for each bucket to precisely exhaust the per-second limit
	{
		results := make([][]Details[int, string], buckets*int(perSecond.count))
		resultIndex := 0

		var wg sync.WaitGroup
		for bucketID := range buckets {
			for processID := range perSecond.count {
				wg.Add(1)
				go func(bucketID int, processID int64, index int) {
					defer wg.Done()
					allowed, details := limiter.allowWithDetails(bucketID, executionTime)
					require.True(t, allowed, "process %d for bucket %d should be allowed", processID, bucketID)
					require.Len(t, details, 2, "should have details for both limits")
					results[index] = details
				}(bucketID, processID, resultIndex)
				resultIndex++
			}
		}
		wg.Wait()

		// Verify all results have the correct structure
		for i, details := range results {
			require.Equal(t, perSecond, details[0].Limit(), "result %d should have per-second limit", i)
			require.True(t, details[0].Allowed(), "result %d per-second should be allowed", i)
			require.Equal(t, executionTime, details[0].ExecutionTime(), "result %d execution time should match", i)

			require.Equal(t, perMinute, details[1].Limit(), "result %d should have per-minute limit", i)
			require.True(t, details[1].Allowed(), "result %d per-minute should be allowed", i)
			require.Equal(t, executionTime, details[1].ExecutionTime(), "result %d execution time should match", i)
		}
	}

	// Verify that additional requests are rejected, all buckets should be exhausted on per-second limit
	{
		var wg sync.WaitGroup
		results := make([][]Details[int, string], buckets)

		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed, details := limiter.allowWithDetails(bucketID, executionTime)
				require.False(t, allowed, "bucket %d should be exhausted after %d requests", bucketID, perSecond.count)
				require.Len(t, details, 2, "should have details for both limits")
				results[bucketID] = details
			}(bucketID)
		}
		wg.Wait()

		// Verify all results show exhaustion correctly
		for bucketID, details := range results {
			// per-second should be denied
			require.False(t, details[0].Allowed(), "bucket %d per-second should be denied", bucketID)
			require.Equal(t, int64(0), details[0].TokensRemaining(), "bucket %d per-second should have 0 tokens", bucketID)
			require.Equal(t, perSecond, details[0].Limit(), "bucket %d should have per-second limit", bucketID)

			// per-minute still has 1 token
			require.True(t, details[1].Allowed(), "bucket %d per-minute should still allow", bucketID)
			require.Equal(t, int64(1), details[1].TokensRemaining(), "bucket %d per-minute should have 1 token", bucketID)
			require.Equal(t, perMinute, details[1].Limit(), "bucket %d should have per-minute limit", bucketID)
		}
	}

	// Refill per-second limit by advancing time
	executionTime = executionTime.Add(time.Second)

	// Per-second bucket should now have 1 more token available
	{
		var wg sync.WaitGroup
		results := make([][]Details[int, string], buckets)

		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed, details := limiter.allowWithDetails(bucketID, executionTime)
				require.True(t, allowed, "bucket %d should allow request after per-second refill", bucketID)
				require.Len(t, details, 2, "should have details for both limits")
				results[bucketID] = details
			}(bucketID)
		}
		wg.Wait()

		// Verify all results show successful consumption
		for bucketID, details := range results {
			require.True(t, details[0].Allowed(), "bucket %d per-second should allow", bucketID)
			require.Equal(t, perSecond.Count()-1, details[0].TokensRemaining(), "bucket %d per-second should have 1 token remaining", bucketID)

			require.True(t, details[1].Allowed(), "bucket %d per-minute should allow", bucketID)
			require.Equal(t, int64(0), details[1].TokensRemaining(), "bucket %d per-minute should have 0 tokens remaining", bucketID)
		}
	}

	// Now all buckets should be exhausted on per-minute limit
	{
		var wg sync.WaitGroup
		results := make([][]Details[int, string], buckets)

		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed, details := limiter.allowWithDetails(bucketID, executionTime)
				require.False(t, allowed, "bucket %d should be exhausted on per-minute limit", bucketID)
				require.Len(t, details, 2, "should have details for both limits")
				results[bucketID] = details
			}(bucketID)
		}
		wg.Wait()

		// Verify all results show per-minute exhaustion
		for bucketID, details := range results {
			// per-second should still allow
			require.True(t, details[0].Allowed(), "bucket %d per-second should still allow", bucketID)
			require.Equal(t, perSecond.Count()-1, details[0].TokensRemaining(), "bucket %d per-second should have 1 token remaining", bucketID)

			// per-minute should be exhausted
			require.False(t, details[1].Allowed(), "bucket %d per-minute should be denied", bucketID)
			require.Equal(t, int64(0), details[1].TokensRemaining(), "bucket %d per-minute should have 0 tokens", bucketID)
		}
	}

	// Complete refill by advancing to refill both limits
	executionTime = executionTime.Add(time.Minute)

	// Test concurrent access after full refill
	{
		var wg sync.WaitGroup
		results := make([][]Details[int, string], buckets*int(perSecond.count))
		resultIndex := 0

		for bucketID := range buckets {
			for processID := range perSecond.count {
				wg.Add(1)
				go func(bucketID int, processID int64, index int) {
					defer wg.Done()
					allowed, details := limiter.allowWithDetails(bucketID, executionTime)
					require.True(t, allowed, "process %d for bucket %d should be allowed after refill", processID, bucketID)
					require.Len(t, details, 2, "should have details for both limits")
					results[index] = details
				}(bucketID, processID, resultIndex)
				resultIndex++
			}
		}
		wg.Wait()

		// Verify all results after refill
		for i, details := range results {
			require.True(t, details[0].Allowed(), "result %d per-second should be allowed", i)
			require.True(t, details[1].Allowed(), "result %d per-minute should be allowed", i)
		}
	}
}

func TestLimiter_Peek_NeverPersists(t *testing.T) {
	t.Parallel()
	const key = "single-test-bucket"
	keyer := func(input string) string {
		return input
	}
	limit1 := NewLimit(rand.Int63n(9)+1, time.Second)
	limit2 := NewLimit(rand.Int63n(99)+1, time.Second)
	limiter := NewLimiter(keyer, limit1, limit2)

	now := time.Now()

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

func TestLimiter_PeekN_SingleBucket(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()
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

func TestLimiter_Peek_MultipleBuckets_MultipleLimits(t *testing.T) {
	t.Parallel()
	keyer := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	perSecond := NewLimit(2, time.Second)
	perMinute := NewLimit(3, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)
	now := time.Now()

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
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	perSecond := NewLimit(rand.Int63n(9)+1, time.Second)
	perMinute := NewLimit(rand.Int63n(99)+1, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)

	executionTime := time.Now()

	// any number of peeks should be true
	for range 101 {
		allowed, details := limiter.peekWithDetails("test-details", executionTime)
		require.True(t, allowed)
		require.Len(t, details, 2, "should have details for both limits")

		require.Equal(t, perSecond, details[0].Limit(), "should have per-second limit in details")
		require.Equal(t, allowed, details[0].Allowed(), "allowed should match for per-second limit")
		require.Equal(t, executionTime, details[0].ExecutionTime(), "execution time should match for per-second limit")
		require.Equal(t, "test-details", details[0].BucketKey(), "bucket key should match for per-second limit")
		require.Equal(t, perSecond.Count(), details[0].TokensRemaining(), "remaining tokens should match for per-second limit")

		require.Equal(t, perMinute, details[1].Limit(), "should have per-minute limit in details")
		require.Equal(t, allowed, details[1].Allowed(), "allowed should match for per-minute limit")
		require.Equal(t, executionTime, details[1].ExecutionTime(), "execution time should match for per-minute limit")
		require.Equal(t, "test-details", details[1].BucketKey(), "bucket key should match for per-minute limit")
		require.Equal(t, perMinute.Count(), details[1].TokensRemaining(), "remaining tokens should match for per-minute limit")
	}
}

func TestLimiter_Allow_SingleBucket_SingleLimit_Func(t *testing.T) {
	t.Parallel()
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

func TestLimiter_Allow_MultipleBuckets_SingleLimit_Func(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	keyer := func(bucketID int) string {
		return fmt.Sprintf("test-bucket-%d", bucketID)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limitFunc := func(input int) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)
	start := time.Now()

	// Enough concurrent processes for each bucket to precisely exhaust the limit
	{
		var wg sync.WaitGroup
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
	}

	// Verify that additional requests are rejected, all buckets should be exhausted
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed := limiter.allow(bucketID, start)
				require.False(t, allowed, "bucket %d should be exhausted after %d requests", bucketID, limit.count)
			}(bucketID)
		}
		wg.Wait()
	}

	// Complete refill
	now := start.Add(limit.period)
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				for range limit.count {
					allowed := limiter.allow(bucketID, now)
					require.True(t, allowed, "bucket %d should be refilled after 1 second", bucketID)
				}
			}(bucketID)
		}
		wg.Wait()
	}
}

func TestLimiter_AllowWithDetails_Func(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(11, time.Second)
	limitFunc := func(input string) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)

	now := time.Now()

	allow, details := limiter.allowWithDetails("test-details", now)
	require.True(t, allow)
	d := details[0]
	require.Equal(t, limit, d.Limit())
	require.Equal(t, now, d.ExecutionTime())
	require.Equal(t, "test-details", d.BucketKey())
	require.Equal(t, limit.count-1, d.TokensRemaining())
}

func TestLimiter_Peek_SingleBucket_Func(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func TestLimiter_PeekNWithDetails_SingleBucket(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input + "-key"
	}
	perSecond := NewLimit(9, time.Second)
	perHour := NewLimit(99, time.Hour)
	limiter := NewLimiter(keyer, perSecond, perHour)

	now := time.Now()

	{
		request := int64(10)
		allowed, details := limiter.peekNWithDetails("test-allow-with-details", now, request)
		require.False(t, allowed)
		require.Len(t, details, 2, "should have details for both limits")

		d0 := details[0]
		require.False(t, d0.Allowed())
		require.Equal(t, d0.BucketInput(), "test-allow-with-details")
		require.Equal(t, d0.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d0.ExecutionTime(), now)
		require.Equal(t, d0.TokensRequested(), request)
		require.Equal(t, d0.TokensConsumed(), int64(0))
		require.Equal(t, d0.TokensRemaining(), perSecond.count)

		d1 := details[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.BucketInput(), "test-allow-with-details")
		require.Equal(t, d1.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d1.ExecutionTime(), now)
		require.Equal(t, d1.TokensRequested(), request)
		require.Equal(t, d1.TokensConsumed(), int64(0))
		require.Equal(t, d1.TokensRemaining(), perHour.count)
	}

	{
		request := int64(9)
		allowed, details := limiter.peekNWithDetails("test-allow-with-details", now, request)
		require.True(t, allowed)
		require.Len(t, details, 2, "should have details for both limits")

		d0 := details[0]
		require.True(t, d0.Allowed())
		require.Equal(t, d0.BucketInput(), "test-allow-with-details")
		require.Equal(t, d0.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d0.ExecutionTime(), now)
		require.Equal(t, d0.TokensRequested(), request)
		require.Equal(t, d0.TokensConsumed(), int64(0))
		require.Equal(t, d0.TokensRemaining(), perSecond.count)

		d1 := details[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.BucketInput(), "test-allow-with-details")
		require.Equal(t, d1.BucketKey(), "test-allow-with-details-key")
		require.Equal(t, d1.ExecutionTime(), now)
		require.Equal(t, d1.TokensRequested(), request)
		require.Equal(t, d1.TokensConsumed(), int64(0))
		require.Equal(t, d1.TokensRemaining(), perHour.count)
	}
}

func TestLimiter_PeekWithDetails_Func(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limitFunc := func(input string) Limit { return limit }
	limiter := NewLimiterFunc(keyer, limitFunc)

	now := time.Now()

	allowed, details := limiter.peekWithDetails("test-details", now)
	require.True(t, allowed)
	d := details[0]
	require.Equal(t, limit, d.Limit())
	require.Equal(t, now, d.ExecutionTime())
	require.Equal(t, "test-details", d.BucketKey())
	require.Equal(t, limit.count, d.TokensRemaining())
}

func TestLimiter_UsesLimitFunc(t *testing.T) {
	t.Parallel()
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
			d := details[0]
			require.Equal(t, limitFunc(i+1), d.Limit())
		}
	}
	{
		limit := NewLimit(888, time.Second)
		limiter := NewLimiter(keyer, limit)

		for i := range 3 {
			allow, details := limiter.allowWithDetails(i+1, time.Now())
			require.True(t, allow)
			d := details[0]
			require.Equal(t, limit, d.Limit())
		}
	}
}

func TestLimiter_Wait_SingleBucket(t *testing.T) {
	t.Parallel()
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

	// Test 1: Wait with enough time to acquire a token
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken), true
		}

		// Done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitWithCancellation("test", executionTime, deadline, done)
		require.True(t, allow, "should acquire token after waiting")

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)
	}

	// Should not allow again immediately after wait
	ok = limiter.allow("test", executionTime)
	require.False(t, ok, "should not allow again immediately after wait")

	// Test 2: Wait with deadline that expires before token is available
	{
		// Deadline that expires too soon
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken / 2), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitWithCancellation("test", executionTime, deadline, done)
		require.False(t, allow, "should not acquire token if deadline expires before token is available")
	}

	// Test 3: Wait with immediate cancellation
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
			return ch
		}

		allow := limiter.waitWithCancellation("test", executionTime, deadline, done)
		require.False(t, allow, "should not acquire token if context is cancelled immediately")
	}

	// Test 4: Wait with no deadline
	{
		// Deadline that returns no deadline
		deadline := func() (time.Time, bool) {
			return time.Time{}, false
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		{
			allow := limiter.waitWithCancellation("test", executionTime, deadline, done)
			require.True(t, allow, "should acquire token when no deadline is set")
		}

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)

		// Should not allow again immediately
		{
			allow := limiter.allow("test", executionTime)
			require.False(t, allow, "should not allow again immediately after wait")
		}
	}
}

func TestLimiter_WaitN_SingleBucket(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Consume all tokens
	{
		ok := limiter.allowN("test", executionTime, limit.count)
		require.True(t, ok, "should allow initial tokens")
	}
	{
		// Should not allow immediately
		ok := limiter.allowN("test", executionTime, 1)
		require.False(t, ok, "should not allow when tokens exhausted")
	}

	// Test 1: Wait with enough time to acquire tokens
	{
		wait := time.Duration(limit.count) * limit.durationPerToken
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(wait), true
		}

		// Done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
		require.True(t, allow, "should acquire tokens after waiting")

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)
	}

	{
		// Should not allow again immediately after wait
		ok := limiter.allow("test", executionTime)
		require.False(t, ok, "should not allow again immediately after wait")
	}

	// Test 2: Wait with deadline that expires before n tokens are available
	{
		// Deadline that expires too soon
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
		require.False(t, allow, "should not acquire tokens if deadline expires before tokens are available")
	}

	// Test 3: Wait with immediate cancellation
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken * time.Duration(limit.count)), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
			return ch
		}

		allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
		require.False(t, allow, "should not acquire tokens if context is cancelled immediately")
	}

	// Test 4: Wait with no deadline
	{
		// Deadline that returns no deadline
		deadline := func() (time.Time, bool) {
			return time.Time{}, false
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		{
			allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
			require.True(t, allow, "should acquire tokens when no deadline is set")
		}

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)

		// Should not allow again immediately
		{
			allow := limiter.allowN("test", executionTime, limit.count)
			require.False(t, allow, "should not allow again immediately after wait")
		}
	}
}

func TestLimiter_Wait_MultipleBuckets(t *testing.T) {
	t.Parallel()
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
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken), true
		}

		// Done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitWithCancellation(bucketID, executionTime, deadline, done)
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
	t.Parallel()
	keyer := func(input int64) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	const buckets int64 = 3
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

	// Test 1: Multiple goroutines competing for tokens with enough time
	{
		// More goroutines than available tokens to create competition
		tokens := buckets * limit.count
		concurrency := tokens * 3 // oversubscribe by 3x
		results := make([]bool, concurrency)
		var wg sync.WaitGroup

		// Deadline that gives enough time for all tokens to be refilled
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.period), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// Start concurrent waits
		for i := range concurrency {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				bucketID := i % buckets
				results[i] = limiter.waitWithCancellation(bucketID, executionTime, deadline, done)
			}(i)
		}

		wg.Wait()

		// We waited
		executionTime = executionTime.Add(limit.period)

		// Count successes and failures
		var successes, failures int64
		for _, result := range results {
			if result {
				successes++
			} else {
				failures++
			}
		}

		// Exactly limit.count tokens per bucket should succeed, 3 buckets * 2 tokens each
		expectedSuccesses := buckets * limit.count
		require.Equal(t, expectedSuccesses, successes, "expected exactly %d goroutines to acquire tokens", expectedSuccesses)

		// The rest should fail due to competition
		expectedFailures := concurrency - expectedSuccesses
		require.Equal(t, expectedFailures, failures, "expected %d goroutines to fail due to competition", expectedFailures)
	}

	// Test 2: Multiple goroutines with deadline that expires before tokens are available
	{
		concurrency := buckets
		results := make([]bool, concurrency)
		var wg sync.WaitGroup

		// Deadline that expires too soon
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken / 2), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// Start concurrent waits
		for i := range concurrency {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				results[i] = limiter.waitWithCancellation(i, executionTime, deadline, done)
			}(i)
		}

		wg.Wait()

		// All should fail because deadline expires before tokens are available
		for i, result := range results {
			require.False(t, result, "goroutine %d should not acquire token due to early deadline", i)
		}
	}

	// Test 3: Multiple goroutines with immediate cancellation
	{
		concurrency := buckets
		results := make([]bool, concurrency)
		var wg sync.WaitGroup

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.period), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
			return ch
		}

		// Start concurrent waits
		for i := range concurrency {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				results[i] = limiter.waitWithCancellation(i, executionTime, deadline, done)
			}(i)
		}

		wg.Wait()

		// All should fail because context is cancelled immediately
		for i, result := range results {
			require.False(t, result, "goroutine %d should not acquire token due to immediate cancellation", i)
		}
	}
}

func TestLimiter_GetBucketsAndLimits(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	executionTime := time.Now()

	perSecond := NewLimit(rand.Int63n(10)+1, time.Second)
	perMinute := NewLimit(rand.Int63n(100)+1, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)
	allow := limiter.allow("test", executionTime)
	require.True(t, allow, "should allow initial token")

	buckets, limits := limiter.getBucketsAndLimits("test", executionTime, false)
	require.Len(t, buckets, 2, "should have two buckets")
	require.Len(t, limits, 2, "should have two limits")
	require.Equal(t, perSecond, limits[0], "first limit should match")
	require.Equal(t, perMinute, limits[1], "second limit should match")
}

func TestDetails_TokensRequestedAndConsumed(t *testing.T) {
	t.Parallel()

	keyer := func(input string) string { return input }
	limit := NewLimit(5, time.Second)
	limiter := NewLimiter(keyer, limit)

	// Test AllowWithDetails when request is allowed
	t.Run("AllowWithDetails_Allowed", func(t *testing.T) {
		allowed, details := limiter.AllowWithDetails("test-key1")
		require.True(t, allowed, "request should be allowed")
		require.Len(t, details, 1, "should have one detail")

		d := details[0]
		require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(1), d.TokensConsumed(), "should consume 1 token when allowed")
		require.Equal(t, int64(4), d.TokensRemaining(), "should have 4 tokens remaining")
	})

	// Test PeekWithDetails (no consumption)
	t.Run("PeekWithDetails", func(t *testing.T) {
		allowed, details := limiter.PeekWithDetails("test-key2")
		require.True(t, allowed, "peek should show available tokens")
		require.Len(t, details, 1, "should have one detail")

		d := details[0]
		require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(0), d.TokensConsumed(), "should consume 0 tokens on peek")
		require.Equal(t, int64(5), d.TokensRemaining(), "should have all 5 tokens remaining")
	})

	// Test AllowWithDetails when request is denied
	t.Run("AllowWithDetails_Denied", func(t *testing.T) {
		// Exhaust the bucket
		for range limit.count {
			limiter.Allow("test-key3")
		}

		allowed, details := limiter.AllowWithDetails("test-key3")
		require.False(t, allowed, "request should be denied")
		require.Len(t, details, 1, "should have one detail")

		d := details[0]
		require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(0), d.TokensConsumed(), "should consume 0 tokens when denied")
		require.Equal(t, int64(0), d.TokensRemaining(), "should have 0 tokens remaining")
	})
}

func TestLimiter_WaitN_ConsumesCorrectTokens(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(10, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Test 1: Wait should consume exactly 1 token
	t.Run("Wait_Consumes_One_Token", func(t *testing.T) {
		// Verify initial state
		_, initialDetails := limiter.peekWithDetails("test-wait-1", executionTime)
		require.Equal(t, limit.count, initialDetails[0].TokensRemaining(), "should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// Wait should succeed and consume exactly 1 token
		allowed := limiter.waitWithCancellation("test-wait-1", executionTime, deadline, done)
		require.True(t, allowed, "wait should succeed")

		// Verify exactly 1 token was consumed
		_, finalDetails := limiter.peekWithDetails("test-wait-1", executionTime)
		require.Equal(t, limit.count-1, finalDetails[0].TokensRemaining(), "should have consumed exactly 1 token")
	})

	// Test 2: WaitN should consume exactly n tokens
	t.Run("WaitN_Consumes_N_Tokens", func(t *testing.T) {
		const tokensToWait = 3

		// Verify initial state
		_, initialDetails := limiter.peekWithDetails("test-waitn-3", executionTime)
		require.Equal(t, limit.count, initialDetails[0].TokensRemaining(), "should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// WaitN should succeed and consume exactly tokensToWait tokens
		allowed := limiter.waitNWithCancellation("test-waitn-3", executionTime, tokensToWait, deadline, done)
		require.True(t, allowed, "waitN should succeed")

		// Verify exactly tokensToWait tokens were consumed
		_, finalDetails := limiter.peekWithDetails("test-waitn-3", executionTime)
		require.Equal(t, limit.count-tokensToWait, finalDetails[0].TokensRemaining(), "should have consumed exactly %d tokens", tokensToWait)
	})

	// Test 3: WaitN with multiple limits should consume n tokens from all buckets
	t.Run("WaitN_MultipleLimits_Consumes_N_From_All", func(t *testing.T) {
		perSecond := NewLimit(5, time.Second)
		perMinute := NewLimit(20, time.Minute)
		multiLimiter := NewLimiter(keyer, perSecond, perMinute)
		const tokensToWait = 2

		// Verify initial state
		_, initialDetails := multiLimiter.peekWithDetails("test-multi-waitn", executionTime)
		require.Len(t, initialDetails, 2, "should have details for both limits")
		require.Equal(t, perSecond.count, initialDetails[0].TokensRemaining(), "per-second should start with all tokens")
		require.Equal(t, perMinute.count, initialDetails[1].TokensRemaining(), "per-minute should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// WaitN should succeed and consume exactly tokensToWait tokens from both limits
		allowed := multiLimiter.waitNWithCancellation("test-multi-waitn", executionTime, tokensToWait, deadline, done)
		require.True(t, allowed, "waitN should succeed with multiple limits")

		// Verify exactly tokensToWait tokens were consumed from both buckets
		_, finalDetails := multiLimiter.peekWithDetails("test-multi-waitn", executionTime)
		require.Len(t, finalDetails, 2, "should have details for both limits")
		require.Equal(t, perSecond.count-tokensToWait, finalDetails[0].TokensRemaining(), "per-second should have consumed exactly %d tokens", tokensToWait)
		require.Equal(t, perMinute.count-tokensToWait, finalDetails[1].TokensRemaining(), "per-minute should have consumed exactly %d tokens", tokensToWait)
	})

	// Test 4: WaitN that fails should consume zero tokens
	t.Run("WaitN_Fails_Consumes_Zero_Tokens", func(t *testing.T) {
		// First, exhaust the bucket
		for range limit.count {
			limiter.allow("test-fail-waitn", executionTime)
		}

		// Verify bucket is exhausted
		_, exhaustedDetails := limiter.peekWithDetails("test-fail-waitn", executionTime)
		require.Equal(t, int64(0), exhaustedDetails[0].TokensRemaining(), "bucket should be exhausted")

		// Deadline that expires immediately (no time to refill)
		deadline := func() (time.Time, bool) {
			return executionTime, true // expires immediately
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// WaitN should fail and consume zero tokens
		allowed := limiter.waitNWithCancellation("test-fail-waitn", executionTime, 1, deadline, done)
		require.False(t, allowed, "waitN should fail when deadline expires before tokens available")

		// Verify no tokens were consumed
		_, finalDetails := limiter.peekWithDetails("test-fail-waitn", executionTime)
		require.Equal(t, int64(0), finalDetails[0].TokensRemaining(), "should still have 0 tokens after failed wait")
	})

	// Test 5: Verify WaitN with high token count
	t.Run("WaitN_High_Token_Count", func(t *testing.T) {
		bigLimit := NewLimit(50, time.Second)
		bigLimiter := NewLimiter(keyer, bigLimit)
		const tokensToWait = 25

		// Verify initial state
		_, initialDetails := bigLimiter.peekWithDetails("test-big-waitn", executionTime)
		require.Equal(t, bigLimit.count, initialDetails[0].TokensRemaining(), "should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// WaitN should succeed and consume exactly tokensToWait tokens
		allowed := bigLimiter.waitNWithCancellation("test-big-waitn", executionTime, tokensToWait, deadline, done)
		require.True(t, allowed, "waitN should succeed with high token count")

		// Verify exactly tokensToWait tokens were consumed
		_, finalDetails := bigLimiter.peekWithDetails("test-big-waitn", executionTime)
		require.Equal(t, bigLimit.count-tokensToWait, finalDetails[0].TokensRemaining(), "should have consumed exactly %d tokens", tokensToWait)
	})

	// Test 6: Concurrent WaitN should consume correct total tokens
	t.Run("WaitN_Concurrent_Token_Consumption", func(t *testing.T) {
		concurrentLimit := NewLimit(20, time.Second)
		concurrentLimiter := NewLimiter(keyer, concurrentLimit)
		const tokensPerWait = 2
		const numGoroutines = 5             // Will try to consume 10 total tokens
		const expectedSuccesses = int64(10) // 20 / 2 = 10 successful waits possible

		results := make([]bool, numGoroutines)
		var wg sync.WaitGroup

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// Start concurrent waits
		for i := range numGoroutines {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i] = concurrentLimiter.waitNWithCancellation("test-concurrent-waitn", executionTime, tokensPerWait, deadline, done)
			}(i)
		}

		wg.Wait()

		// Count successes
		var successes int64
		for _, result := range results {
			if result {
				successes++
			}
		}

		require.Equal(t, expectedSuccesses/tokensPerWait, successes, "expected exactly %d successful waits", expectedSuccesses/tokensPerWait)

		// Verify total tokens consumed
		_, finalDetails := concurrentLimiter.peekWithDetails("test-concurrent-waitn", executionTime)
		expectedRemaining := concurrentLimit.count - (successes * tokensPerWait)
		require.Equal(t, expectedRemaining, finalDetails[0].TokensRemaining(), "should have consumed exactly %d tokens total", successes*tokensPerWait)
	})
}

func TestLimiter_Wait_FIFO_Ordering_SingleBucket(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return "test-bucket"
	}
	// 1 token per 50ms
	limit := NewLimit(1, 50*time.Millisecond)
	limiter := NewLimiter(keyer, limit)
	executionTime := time.Now()

	// Exhaust the single token
	require.True(t, limiter.allow("key", executionTime), "should allow initial token")
	require.False(t, limiter.allow("key", executionTime), "should not allow second token")

	const concurrency = 5
	var wg sync.WaitGroup
	wg.Add(concurrency)

	startOrder := make(chan int, concurrency)
	successOrder := make(chan int, concurrency)

	// Deadline that gives enough time for all tokens to be refilled
	deadline := func() (time.Time, bool) {
		return executionTime.Add(time.Duration(concurrency) * limit.durationPerToken), true
	}

	// Done channel that never closes
	done := func() <-chan struct{} {
		return make(chan struct{}) // never closes
	}

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()

			// Signal that this goroutine is starting its wait
			startOrder <- id

			// Wait for a token
			if limiter.waitWithCancellation("key", executionTime, deadline, done) {
				// Signal that this goroutine successfully acquired a token
				successOrder <- id
			}
		}(i)
		// A small, non-deterministic delay to encourage goroutines to queue up in order.
		// This helps simulate a real-world scenario where requests arrive sequentially.
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()
	close(startOrder)
	close(successOrder)

	var starts []int
	for id := range startOrder {
		starts = append(starts, id)
	}

	var successes []int
	for id := range successOrder {
		successes = append(successes, id)
	}

	require.Equal(t, concurrency, len(successes), "all goroutines should have acquired a token")
	require.Equal(t, starts, successes, "success order should match start order, proving FIFO")
}

func TestLimiter_Wait_FIFO_Ordering_MultipleBuckets(t *testing.T) {
	t.Parallel()

	const buckets = 3
	const concurrencyPerBucket = 5
	const concurrency = buckets * concurrencyPerBucket

	keyer := func(input int) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	// 1 token per 50ms, to make the test run reasonably fast
	limit := NewLimit(1, 50*time.Millisecond)
	limiter := NewLimiter(keyer, limit)
	executionTime := time.Now()

	// Exhaust the single token for each bucket
	for i := range buckets {
		require.True(t, limiter.allow(i, executionTime), "should allow initial token for bucket %d", i)
		require.False(t, limiter.allow(i, executionTime), "should not allow second token for bucket %d", i)
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)

	// Create maps to hold order channels for each bucket
	startOrders := make(map[int]chan int, buckets)
	successOrders := make(map[int]chan int, buckets)
	for i := range buckets {
		startOrders[i] = make(chan int, concurrencyPerBucket)
		successOrders[i] = make(chan int, concurrencyPerBucket)
	}

	// Deadline that gives enough time for all tokens to be refilled for all goroutines
	deadline := func() (time.Time, bool) {
		return executionTime.Add(time.Duration(concurrencyPerBucket) * limit.durationPerToken), true
	}

	// Done channel that never closes
	done := func() <-chan struct{} {
		return make(chan struct{}) // never closes
	}

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()
			bucketID := id % buckets

			// Announce that this goroutine is starting its wait for its bucket
			startOrders[bucketID] <- id

			// Wait for a token
			if limiter.waitWithCancellation(bucketID, executionTime, deadline, done) {
				// Announce that this goroutine successfully acquired a token
				successOrders[bucketID] <- id
			}
		}(i)
		// A small, non-deterministic delay to encourage goroutines to queue up in order.
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()

	// Close all channels
	for bucketID := range buckets {
		close(startOrders[bucketID])
		close(successOrders[bucketID])
	}

	// Verify FIFO order for each bucket
	for bucketID := range buckets {
		var starts []int
		for id := range startOrders[bucketID] {
			starts = append(starts, id)
		}

		var successes []int
		for id := range successOrders[bucketID] {
			successes = append(successes, id)
		}

		require.Equal(t, concurrencyPerBucket, len(successes), "all goroutines for bucket %d should have acquired a token", bucketID)
		require.Equal(t, starts, successes, "success order should match start order for bucket %d, proving FIFO", bucketID)
	}
}

func TestLimiter_WaitersCleanup_Basic(t *testing.T) {
	t.Parallel()

	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(1, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	// Initial waiters count should be 0
	require.Equal(t, 0, limiter.waiters.count(), "initial waiters count should be 0")

	executionTime := time.Now()

	// Exhaust tokens for multiple keys
	require.True(t, limiter.allow("key1", executionTime), "should allow initial token for key1")
	require.True(t, limiter.allow("key2", executionTime), "should allow initial token for key2")

	// Create deadline that will timeout immediately
	deadline := func() (time.Time, bool) {
		return executionTime, true // immediate timeout
	}
	done := func() <-chan struct{} {
		ch := make(chan struct{})
		close(ch) // immediately cancelled
		return ch
	}

	// Try to wait - this should create a waiter entry that gets cleaned up
	allow := limiter.waitWithCancellation("key1", executionTime, deadline, done)
	require.False(t, allow, "should timeout immediately")

	// Check if waiter was cleaned up
	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after timeout")

	// Try again with a different key
	allow = limiter.waitWithCancellation("key2", executionTime, deadline, done)
	require.False(t, allow, "should timeout immediately")

	// Check waiters count again
	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after timeout")

	// Try the same key again
	allow = limiter.waitWithCancellation("key1", executionTime, deadline, done)
	require.False(t, allow, "should timeout immediately")

	// Check waiters count
	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after timeout")
}

func TestLimiter_WaitersCleanup_MemoryLeak_Prevention(t *testing.T) {
	t.Parallel()

	keyer := func(input int) string {
		return fmt.Sprintf("key-%d", input)
	}
	limit := NewLimit(1, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Create deadline that will timeout immediately
	deadline := func() (time.Time, bool) {
		return executionTime, true // immediate timeout
	}
	done := func() <-chan struct{} {
		ch := make(chan struct{})
		close(ch) // immediately cancelled
		return ch
	}

	const numKeys = 1000

	// Simulate many different keys trying to wait
	for i := range numKeys {
		// First exhaust the token for this key
		limiter.allow(i, executionTime)

		// Then try to wait (which will timeout immediately)
		result := limiter.waitWithCancellation(i, executionTime, deadline, done)
		require.False(t, result, "should timeout immediately for key %d", i)
	}

	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after use")
}

func TestLimiter_WaitersCleanup_Concurrent(t *testing.T) {
	t.Parallel()

	keyer := func(input int) string {
		return fmt.Sprintf("key-%d", input)
	}
	limit := NewLimit(1, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Create deadline that will timeout immediately
	deadline := func() (time.Time, bool) {
		return executionTime, true // immediate timeout
	}
	done := func() <-chan struct{} {
		ch := make(chan struct{})
		close(ch) // immediately cancelled
		return ch
	}

	const concurrency = 100
	const keysPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := range concurrency {
		go func(i int) {
			defer wg.Done()

			for k := range keysPerGoroutine {
				keyID := i*keysPerGoroutine + k

				// First exhaust the token for this key
				limiter.allow(keyID, executionTime)

				// Then try to wait (which will timeout immediately)
				limiter.waitWithCancellation(keyID, executionTime, deadline, done)
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after use")
}

func TestLimiter_WaitersCleanup_WithSuccessfulWaits(t *testing.T) {
	t.Parallel()

	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(1, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Exhaust the token
	require.True(t, limiter.allow("key", executionTime), "should allow initial token")

	const concurrency = 20
	results := make([]bool, concurrency)

	// Create deadline that gives enough time for tokens to be refilled
	deadline := func() (time.Time, bool) {
		return executionTime.Add(time.Duration(concurrency) * limit.durationPerToken), true
	}

	done := func() <-chan struct{} {
		return make(chan struct{}) // never closes
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := range concurrency {
		go func(i int) {
			defer wg.Done()
			// stagger the start times
			time.Sleep(time.Duration(i) * time.Millisecond)
			results[i] = limiter.waitWithCancellation("key", executionTime, deadline, done)
		}(i)
	}
	wg.Wait()

	// All waiters should have eventually succeeded
	successes := 0
	for _, result := range results {
		if result {
			successes++
		}
	}

	require.Equal(t, concurrency, successes, "all waiters should eventually succeed")
	require.Equal(t, 0, limiter.waiters.count(), "all waiters should be cleaned up after completion")
}

func TestLimiter_Wait_FIFOOrdering_HighContention(t *testing.T) {
	t.Parallel()

	keyer := func(input int) int {
		return input
	}

	limiter := NewLimiter(keyer, NewLimit(1, 50*time.Millisecond))

	// Exhaust the bucket
	require.True(t, limiter.Allow(1))

	// Test that multiple waiters can successfully acquire tokens
	const waiters = 5
	results := make(chan bool, waiters)

	for range waiters {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			results <- limiter.Wait(ctx, 1)
		}()
	}

	// Collect results
	successes := 0
	for range waiters {
		select {
		case success := <-results:
			if success {
				successes++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Test timed out")
		}
	}

	// Should get at least some successes (the fix should prevent token starvation)
	require.Greater(t, successes, 0, "Should have some successful token acquisitions")
	require.Equal(t, 0, limiter.waiters.count(), "All waiters should be cleaned up")
}

func TestLimiter_Wait_RaceCondition_Prevention(t *testing.T) {
	t.Parallel()

	keyer := func(input int) int {
		return input
	}
	// Use a very restrictive limit to force contention
	limiter := NewLimiter(keyer, NewLimit(1, 100*time.Millisecond))

	// Exhaust the bucket
	require.True(t, limiter.Allow(1))

	const concurrency = 50
	successes := int64(0)

	var wg sync.WaitGroup
	wg.Add(concurrency)
	// Start many goroutines that will compete for tokens
	for range concurrency {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			if limiter.Wait(ctx, 1) {
				atomic.AddInt64(&successes, 1)
			}
		}()
	}
	wg.Wait()

	// Should have some successes but not more than the tokens available
	// in the timeout period (roughly 20 tokens in 2 seconds at 100ms per token)
	actual := atomic.LoadInt64(&successes)
	require.Greater(t, actual, int64(0), "should have some successful acquisitions")
	require.LessOrEqual(t, actual, int64(25), "should not exceed reasonable token availability")

	// Verify no memory leaks
	require.Equal(t, 0, limiter.waiters.count(), "all waiters should be cleaned up")
}
