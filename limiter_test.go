package rate

import (
	"fmt"
	"math/rand"
	"sync"
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
	require.Equal(t, limiter.buckets.Count(), expected, "buckets should have persisted after allow")
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
	require.Equal(t, limiter.buckets.Count(), 0, "buckets should not persist after peeking")
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
		// Exhaust the bucket first
		for i := 0; i < 5; i++ {
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
