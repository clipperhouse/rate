package rate

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/clipperhouse/ntime"
	"github.com/stretchr/testify/require"
)

func TestLimiter_Allow(t *testing.T) {
	t.Parallel()

	t.Run("SingleBucket", func(t *testing.T) {
		t.Parallel()

		t.Run("SingleLimit", func(t *testing.T) {
			t.Parallel()

			t.Run("Basic", func(t *testing.T) {
				t.Parallel()
				keyFunc := func(input string) string {
					return input
				}
				limit := NewLimit(9, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				now := ntime.Now()

				for range limit.count {
					require.True(t, limiter.allow("test", now))
				}

				require.False(t, limiter.allow("test", now))

				// A token is ~111ms
				now = now.Add(limit.durationPerToken)

				require.True(t, limiter.allow("test", now))
			})

			t.Run("Func", func(t *testing.T) {
				t.Parallel()
				keyFunc := func(input string) string {
					return input
				}
				limit := NewLimit(9, time.Second)
				limitFunc := func(input string) Limit { return limit }
				limiter := NewLimiterFunc(keyFunc, limitFunc)

				now := ntime.Now()

				for range limit.count {
					require.True(t, limiter.allow("test", now))
				}

				require.False(t, limiter.allow("test", now))

				// A token is ~111ms
				now = now.Add(limit.durationPerToken)

				require.True(t, limiter.allow("test", now))
			})
		})

		t.Run("MultipleLimits", func(t *testing.T) {
			t.Parallel()

			t.Run("Basic", func(t *testing.T) {
				t.Parallel()
				keyFunc := func(input string) string {
					return input
				}

				// Set up a limiter with multiple limits:
				// - 2 requests per second (more restrictive)
				// - 3 requests per minute (less restrictive per-second, but more restrictive overall)
				// This demonstrates that ALL limits must allow for a token to be consumed
				perSecond := NewLimit(2, time.Second)
				perMinute := NewLimit(3, time.Minute)
				limiter := NewLimiter(keyFunc, perSecond, perMinute)

				now := ntime.Now()

				// Consume tokens up to the per-second limit
				calls := perSecond.Count() // 2
				for i := range calls {
					allowed := limiter.allow("test", now)
					require.True(t, allowed, "should allow token %d", i+1)
				}

				//...leaving 0 in the per-second bucket and 1 in the per-minute bucket

				// 2 tokens should have been consumed from both buckets
				_, details1 := limiter.peekWithDebug("test", now)
				require.Equal(t, perSecond.Count()-calls, details1[0].TokensRemaining(), "per-second bucket should be exhausted")
				require.Equal(t, perMinute.Count()-calls, details1[1].TokensRemaining(), "per-minute bucket should have 1 token remaining")

				// Per-second limit exhausted, but per-minute has 1 token left,
				// should fail because we require all limits to allow
				allowed := limiter.allow("test", now)
				require.False(t, allowed, "should not allow when per-second limit is exhausted")

				// Verify that no tokens were consumed from any bucket,
				// because the above request was denied
				_, details2 := limiter.peekWithDebug("test", now)
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
				_, details3 := limiter.peekWithDebug("test", now)
				require.Equal(t, int64(1), details3[0].TokensRemaining(), "per-second bucket should be unchanged after denial")
				require.Equal(t, int64(0), details3[1].TokensRemaining(), "per-minute bucket should remain exhausted")

				// Refill both buckets by advancing time
				now = now.Add(time.Minute)

				// Should work now that per-minute is refilled
				allowed = limiter.allow("test", now)
				require.True(t, allowed, "should allow after per-minute bucket refills")
			})
		})

		t.Run("AlwaysPersists", func(t *testing.T) {
			t.Parallel()
			keyFunc := func(input int) string {
				return fmt.Sprintf("bucket-allow-always-persists-%d", input)
			}
			limit1 := NewLimit(9, time.Second)
			limit2 := NewLimit(99, time.Second)
			limiter := NewLimiter(keyFunc, limit1, limit2)
			const buckets = 3

			now := ntime.Now()

			for bucketID := range buckets {
				limiter.allow(bucketID, now)
			}

			expected := buckets * len(limiter.limits)
			require.Equal(t, int64(expected), limiter.buckets.count(), "buckets should have persisted after allow")
		})
	})

	t.Run("MultipleBuckets", func(t *testing.T) {
		t.Parallel()

		t.Run("SingleLimit", func(t *testing.T) {
			t.Parallel()

			t.Run("Basic", func(t *testing.T) {
				t.Parallel()
				keyFunc := func(input int) string {
					return fmt.Sprintf("test-bucket-%d", input)
				}
				const buckets = 3

				limit := NewLimit(9, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				now := ntime.Now()

				for bucketID := range buckets {
					for range 9 {
						require.True(t, limiter.allow(bucketID, now))
					}
					require.False(t, limiter.allow(bucketID, now))
				}
			})

			t.Run("Func", func(t *testing.T) {
				t.Parallel()
				keyFunc := func(input string) string {
					return input
				}
				const buckets = 3
				limit := NewLimit(9, time.Second)
				limitFunc := func(input string) Limit { return limit }
				limiter := NewLimiterFunc(keyFunc, limitFunc)
				now := ntime.Now()

				for i := range buckets {
					for range 9 {
						limiter.allow(fmt.Sprintf("test-%d", i), now)
					}
					require.False(t, limiter.allow(fmt.Sprintf("test-%d", i), now))
				}
			})

			t.Run("Concurrent", func(t *testing.T) {
				t.Parallel()
				keyFunc := func(bucketID int) string {
					return fmt.Sprintf("test-bucket-%d", bucketID)
				}
				const buckets = 3
				limit := NewLimit(9, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				start := ntime.Now()

				// Enough concurrent processes for each bucket to precisely exhaust the limit
				{
					var wg sync.WaitGroup
					for bucketID := range buckets {
						for processID := range limit.count {
							wg.Add(1)
							go func(bucketID int, processID int64) {
								defer wg.Done()
								allowed := limiter.allow(bucketID, start)
								require.True(t, allowed, "process %d for bucket %s should be allowed", processID, keyFunc(bucketID))
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
				t.Run("Func", func(t *testing.T) {
					t.Parallel()
					keyFunc := func(bucketID int) string {
						return fmt.Sprintf("test-bucket-%d", bucketID)
					}
					const buckets = 3
					limit := NewLimit(9, time.Second)
					limitFunc := func(input int) Limit { return limit }
					limiter := NewLimiterFunc(keyFunc, limitFunc)
					start := ntime.Now()

					// Enough concurrent processes for each bucket to precisely exhaust the limit
					{
						var wg sync.WaitGroup
						for bucketID := range buckets {
							for processID := range limit.count {
								wg.Add(1)
								go func(bucketID int, processID int64) {
									defer wg.Done()
									allowed := limiter.allow(bucketID, start)
									require.True(t, allowed, "process %d for bucket %s should be allowed", processID, keyFunc(bucketID))
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
				})
			})

		})

		t.Run("MultipleLimits", func(t *testing.T) {
			t.Parallel()

			t.Run("Basic", func(t *testing.T) {
				t.Parallel()
				keyFunc := func(input int) string {
					return fmt.Sprintf("test-bucket-%d", input)
				}
				const buckets = 2
				perSecond := NewLimit(2, time.Second)
				perMinute := NewLimit(3, time.Minute)
				limiter := NewLimiter(keyFunc, perSecond, perMinute)
				executionTime := ntime.Now()

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
			})

			t.Run("Concurrent", func(t *testing.T) {
				t.Parallel()
				keyFunc := func(bucketID int) string {
					return fmt.Sprintf("test-bucket-%d", bucketID)
				}
				const buckets = 3
				perSecond := NewLimit(2, time.Second)
				perMinute := NewLimit(3, time.Minute)
				limiter := NewLimiter(keyFunc, perSecond, perMinute)
				executionTime := ntime.Now()

				// Enough concurrent processes for each bucket to precisely exhaust the per-second limit
				{
					var wg sync.WaitGroup
					for bucketID := range buckets {
						for processID := range perSecond.count {
							wg.Add(1)
							go func(bucketID int, processID int64) {
								defer wg.Done()
								allowed := limiter.allow(bucketID, executionTime)
								require.True(t, allowed, "process %d for bucket %s should be allowed", processID, keyFunc(bucketID))
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
								require.True(t, allowed, "process %d for bucket %s should be allowed after refill", processID, keyFunc(bucketID))
							}(bucketID, processID)
						}
					}
					wg.Wait()
				}
			})
		})
	})
}

func TestLimiter_AllowN_SingleBucket(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyFunc, limit)

	now := ntime.Now()
	require.True(t, limiter.allowN("test", now, 1))
	require.True(t, limiter.allowN("test", now, 8))
	require.False(t, limiter.allowN("test", now, 1))

	now = now.Add(3 * limit.durationPerToken)
	require.True(t, limiter.allowN("test", now, 2))
	require.True(t, limiter.allowN("test", now, 1))
	require.False(t, limiter.allowN("test", now, 1))
}

func TestLimiter_AllowNWithDebug_SingleBucket(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input + "-key"
	}
	perSecond := NewLimit(9, time.Second)
	perHour := NewLimit(99, time.Hour)
	limiter := NewLimiter(keyFunc, perSecond, perHour)

	now := ntime.Now()

	// previously consumed, running tally
	consumed := int64(0)

	{
		consume := int64(3)
		allowed, debugs := limiter.allowNWithDebug("test-allow-with-debug", now, consume)
		require.True(t, allowed)
		require.Len(t, debugs, 2, "should have debugs for both limits")

		d0 := debugs[0]
		require.True(t, d0.Allowed())
		require.Equal(t, d0.Input(), "test-allow-with-debug")
		require.Equal(t, d0.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d0.ExecutionTime(), now.ToTime())
		require.Equal(t, d0.TokensRequested(), consume)
		require.Equal(t, d0.TokensConsumed(), consume)
		require.Equal(t, d0.TokensRemaining(), perSecond.count-consume)
		require.Equal(t, time.Duration(0), d0.RetryAfter(), "per-second RetryAfter should be 0 when allowed")

		d1 := debugs[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.Input(), "test-allow-with-debug")
		require.Equal(t, d1.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d1.ExecutionTime(), now.ToTime())
		require.Equal(t, d1.TokensRequested(), consume)
		require.Equal(t, d1.TokensConsumed(), consume)
		require.Equal(t, d1.TokensRemaining(), perHour.count-consume)
		require.Equal(t, time.Duration(0), d1.RetryAfter(), "per-hour RetryAfter should be 0 when allowed")

		consumed += consume
	}

	// should still be ok
	{
		consume := int64(6)
		allowed, debugs := limiter.allowNWithDebug("test-allow-with-debug", now, consume)
		require.True(t, allowed)
		require.Len(t, debugs, 2, "should have debugs for both limits")

		d0 := debugs[0]
		require.True(t, d0.Allowed())
		require.Equal(t, d0.Input(), "test-allow-with-debug")
		require.Equal(t, d0.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d0.ExecutionTime(), now.ToTime())
		require.Equal(t, d0.TokensRequested(), consume)
		require.Equal(t, d0.TokensConsumed(), consume)
		require.Equal(t, d0.TokensRemaining(), perSecond.count-consume-consumed)
		require.Equal(t, time.Duration(0), d0.RetryAfter(), "per-second RetryAfter should be 0 when allowed")

		d1 := debugs[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.Input(), "test-allow-with-debug")
		require.Equal(t, d1.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d1.ExecutionTime(), now.ToTime())
		require.Equal(t, d1.TokensRequested(), consume)
		require.Equal(t, d1.TokensConsumed(), consume)
		require.Equal(t, d1.TokensRemaining(), perHour.count-consume-consumed)
		require.Equal(t, time.Duration(0), d1.RetryAfter(), "per-hour RetryAfter should be 0 when allowed")

		consumed += consume
	}

	// per-second now exhausted, should be denied, no tokens consumed
	{
		consume := int64(2)
		allowed, debugs := limiter.allowNWithDebug("test-allow-with-debug", now, consume)
		require.False(t, allowed)
		require.Len(t, debugs, 2, "should have debugs for both limits")

		d0 := debugs[0]
		require.False(t, d0.Allowed())
		require.Equal(t, d0.Input(), "test-allow-with-debug")
		require.Equal(t, d0.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d0.ExecutionTime(), now.ToTime())
		require.Equal(t, d0.TokensRequested(), consume)
		require.Equal(t, d0.TokensConsumed(), int64(0))
		require.Equal(t, d0.TokensRemaining(), perSecond.count-consumed)
		// per-second limit is exhausted, so we need to wait for 2 tokens to refill
		rounding := time.Nanosecond
		require.Equal(t, 2*perSecond.durationPerToken, d0.RetryAfter()+rounding, "per-second RetryAfter should be time to refill %d tokens", consume)

		d1 := debugs[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.Input(), "test-allow-with-debug")
		require.Equal(t, d1.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d1.ExecutionTime(), now.ToTime())
		require.Equal(t, d1.TokensRequested(), consume)
		require.Equal(t, d1.TokensConsumed(), int64(0))
		require.Equal(t, d1.TokensRemaining(), perHour.count-consumed)
		require.Equal(t, time.Duration(0), d1.RetryAfter(), "per-hour RetryAfter should be 0 when tokens are available")

		consumed += 0
	}

	refilled := int64(3)
	now = now.Add(time.Duration(refilled) * perSecond.durationPerToken)

	// per-second refilled
	{
		consume := int64(2)
		allowed, debugs := limiter.allowNWithDebug("test-allow-with-debug", now, consume)
		require.True(t, allowed)
		require.Len(t, debugs, 2, "should have debugs for both limits")

		d0 := debugs[0]
		require.True(t, d0.Allowed())
		require.Equal(t, d0.Input(), "test-allow-with-debug")
		require.Equal(t, d0.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d0.ExecutionTime(), now.ToTime())
		require.Equal(t, d0.TokensRequested(), consume)
		require.Equal(t, d0.TokensConsumed(), consume)
		require.Equal(t, d0.TokensRemaining(), perSecond.count-consume-consumed+refilled)
		require.Equal(t, time.Duration(0), d0.RetryAfter(), "per-second RetryAfter should be 0 when allowed after refill")

		d1 := debugs[1]
		require.True(t, d1.Allowed())
		require.Equal(t, d1.Input(), "test-allow-with-debug")
		require.Equal(t, d1.Key(), "test-allow-with-debug-key")
		// require.Equal(t, d1.ExecutionTime(), now.ToTime())
		require.Equal(t, d1.TokensRequested(), consume)
		require.Equal(t, d1.TokensConsumed(), consume)
		require.Equal(t, d1.TokensRemaining(), perHour.count-consume-consumed+0) // time passing not enough to refill per-hour
		require.Equal(t, time.Duration(0), d1.RetryAfter(), "per-hour RetryAfter should be 0 when allowed")
	}
}

func TestLimiter_AllowWithDebug_SingleBucket_MultipleLimits(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input
	}
	perSecond := NewLimit(2, time.Second)
	perMinute := NewLimit(3, time.Minute)
	limiter := NewLimiter(keyFunc, perSecond, perMinute)

	const bucketID = "test-allow-with-debug"

	executionTime := ntime.Now()
	{
		// exhaust the per-second limit, but per-minute should still have 1 token
		for i := range perSecond.Count() {
			allowed, debugs := limiter.allowWithDebug(bucketID, executionTime)
			require.True(t, allowed)
			require.Len(t, debugs, 2, "should have debugs for both limits")

			d0 := debugs[0]
			require.Equal(t, perSecond, d0.Limit(), "should have per-second limit in debug")
			require.Equal(t, allowed, d0.Allowed(), "allowed should match for per-second limit")
			// require.Equal(t, executionTime.ToTime(), d0.ExecutionTime(), "execution time should match for per-second limit")
			require.Equal(t, bucketID, d0.Input(), "input should match for per-second limit")
			require.Equal(t, bucketID, d0.Key(), "bucket key should match for per-second limit")
			require.Equal(t, int64(1), d0.TokensRequested(), "per-second limit should request 1 token")
			require.Equal(t, int64(1), d0.TokensConsumed(), "per-second limit should consume 1 token when allowed")
			require.Equal(t, perSecond.Count()-i-1, d0.TokensRemaining(), "remaining tokens should match for per-second limit")
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "per-second RetryAfter should be 0 when allowed")

			d1 := debugs[1]
			require.Equal(t, perMinute, d1.Limit(), "should have per-minute limit in debug")
			require.Equal(t, allowed, d1.Allowed(), "allowed should match for per-minute limit")
			// require.Equal(t, executionTime.ToTime(), d1.ExecutionTime(), "execution time should match for per-minute limit")
			require.Equal(t, bucketID, d1.Input(), "input should match for per-minute limit")
			require.Equal(t, bucketID, d1.Key(), "bucket key should match for per-minute limit")
			require.Equal(t, int64(1), d1.TokensRequested(), "per-minute limit should request 1 token")
			require.Equal(t, int64(1), d1.TokensConsumed(), "per-minute limit should consume 1 token when allowed")
			require.Equal(t, perMinute.Count()-i-1, d1.TokensRemaining(), "remaining tokens should match for per-minute limit")
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "per-minute RetryAfter should be 0 when allowed")
		}
	}

	{
		allowed, debugs := limiter.allowWithDebug(bucketID, executionTime)
		require.False(t, allowed)
		require.Len(t, debugs, 2, "should have debugs for both limits")

		// per-second should have been denied
		d0 := debugs[0]
		require.False(t, d0.Allowed(), "allowed should match for per-second limit")
		require.Equal(t, int64(0), d0.TokensRemaining(), "remaining tokens should match for per-second limit")
		require.Equal(t, perSecond, d0.Limit(), "should have per-second limit in debug")
		require.Equal(t, executionTime.ToTime(), d0.ExecutionTime(), "execution time should match for per-second limit")
		require.Equal(t, bucketID, d0.Input(), "input should match for per-second limit")
		require.Equal(t, bucketID, d0.Key(), "bucket key should match for per-second limit")
		require.Equal(t, int64(1), d0.TokensRequested(), "per-second limit should request 1 token")
		require.Equal(t, int64(0), d0.TokensConsumed(), "per-second limit should consume 0 tokens when denied")
		// per-second limit is exhausted, so we need to wait for 1 token to refill
		expectedRetryAfter := perSecond.durationPerToken
		require.Equal(t, expectedRetryAfter, d0.RetryAfter(), "per-second RetryAfter should be time to refill 1 token")

		// per-minute still has 1 token
		d1 := debugs[1]
		require.True(t, d1.Allowed(), "allowed should match for per-minute limit")
		require.Equal(t, int64(1), d1.TokensRemaining(), "per-minute limit should have 1 remaining token")
		require.Equal(t, perMinute, d1.Limit(), "should have per-minute limit in debug")
		require.Equal(t, executionTime.ToTime(), d1.ExecutionTime(), "execution time should match for per-minute limit")
		require.Equal(t, bucketID, d1.Input(), "input should match for per-minute limit")
		require.Equal(t, bucketID, d1.Key(), "bucket key should match for per-minute limit")
		require.Equal(t, int64(1), d1.TokensRequested(), "per-minute limit should request 1 token")
		require.Equal(t, int64(0), d1.TokensConsumed(), "per-minute limit should consume 0 tokens when overall request is denied")
		require.Equal(t, time.Duration(0), d1.RetryAfter(), "per-minute RetryAfter should be 0 when tokens are available")
	}
}

func TestLimiter_AllowWithDebug_MultipleBuckets_MultipleLimits_Concurrent(t *testing.T) {
	t.Parallel()
	keyFunc := func(bucketID int) string {
		return fmt.Sprintf("test-bucket-%d", bucketID)
	}
	const buckets = 3
	perSecond := NewLimit(2, time.Second)
	perMinute := NewLimit(3, time.Minute)
	limiter := NewLimiter(keyFunc, perSecond, perMinute)
	executionTime := ntime.Now()

	// Enough concurrent processes for each bucket to precisely exhaust the per-second limit
	{
		results := make([][]Debug[int, string], buckets*int(perSecond.count))
		resultIndex := 0

		var wg sync.WaitGroup
		for bucketID := range buckets {
			for processID := range perSecond.count {
				wg.Add(1)
				go func(bucketID int, processID int64, index int) {
					defer wg.Done()
					allowed, debugs := limiter.allowWithDebug(bucketID, executionTime)
					require.True(t, allowed, "process %d for bucket %d should be allowed", processID, bucketID)
					require.Len(t, debugs, 2, "should have debugs for both limits")
					results[index] = debugs
				}(bucketID, processID, resultIndex)
				resultIndex++
			}
		}
		wg.Wait()

		// Verify all results have the correct structure
		for i, debugs := range results {
			bucketID := i / int(perSecond.count) // Calculate bucketID from index
			expectedKey := fmt.Sprintf("test-bucket-%d", bucketID)

			d0 := debugs[0]
			require.Equal(t, perSecond, d0.Limit(), "debug %d should have per-second limit", i)
			require.True(t, d0.Allowed(), "debug %d per-second should be allowed", i)
			require.Equal(t, executionTime.ToTime(), d0.ExecutionTime(), "debug %d execution time should match", i)
			require.Equal(t, bucketID, d0.Input(), "debug %d input should match", i)
			require.Equal(t, expectedKey, d0.Key(), "debug %d key should match", i)
			require.Equal(t, int64(1), d0.TokensRequested(), "debug %d should request 1 token", i)
			require.Equal(t, int64(1), d0.TokensConsumed(), "debug %d should consume 1 token when allowed", i)
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "debug %d RetryAfter should be 0 when allowed", i)

			d1 := debugs[1]
			require.Equal(t, perMinute, d1.Limit(), "debug %d should have per-minute limit", i)
			require.True(t, d1.Allowed(), "debug %d per-minute should be allowed", i)
			require.Equal(t, executionTime.ToTime(), d1.ExecutionTime(), "debug %d execution time should match", i)
			require.Equal(t, bucketID, d1.Input(), "debug %d input should match", i)
			require.Equal(t, expectedKey, d1.Key(), "debug %d key should match", i)
			require.Equal(t, int64(1), d1.TokensRequested(), "debug %d should request 1 token", i)
			require.Equal(t, int64(1), d1.TokensConsumed(), "debug %d should consume 1 token when allowed", i)
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "debug %d RetryAfter should be 0 when allowed", i)
		}
	}

	// Verify that additional requests are rejected, all buckets should be exhausted on per-second limit
	{
		results := make([][]Debug[int, string], buckets)

		var wg sync.WaitGroup
		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed, debugs := limiter.allowWithDebug(bucketID, executionTime)
				require.False(t, allowed, "bucket %d should be exhausted after %d requests", bucketID, perSecond.count)
				require.Len(t, debugs, 2, "should have debugs for both limits")
				results[bucketID] = debugs
			}(bucketID)
		}
		wg.Wait()

		// Verify all results show exhaustion correctly
		for bucketID, debugs := range results {
			expectedKey := fmt.Sprintf("test-bucket-%d", bucketID)

			// per-second should be denied
			d0 := debugs[0]
			require.False(t, d0.Allowed(), "bucket %d per-second should be denied", bucketID)
			require.Equal(t, int64(0), d0.TokensRemaining(), "bucket %d per-second should have 0 tokens", bucketID)
			require.Equal(t, perSecond, d0.Limit(), "bucket %d should have per-second limit", bucketID)
			require.Equal(t, executionTime.ToTime(), d0.ExecutionTime(), "bucket %d execution time should match", bucketID)
			require.Equal(t, bucketID, d0.Input(), "bucket %d input should match", bucketID)
			require.Equal(t, expectedKey, d0.Key(), "bucket %d key should match", bucketID)
			require.Equal(t, int64(1), d0.TokensRequested(), "bucket %d should request 1 token", bucketID)
			require.Equal(t, int64(0), d0.TokensConsumed(), "bucket %d should consume 0 tokens when denied", bucketID)
			require.Equal(t, perSecond.durationPerToken, d0.RetryAfter(), "bucket %d RetryAfter should be time to refill 1 token", bucketID)

			// per-minute still has 1 token
			d1 := debugs[1]
			require.True(t, d1.Allowed(), "bucket %d per-minute should still allow", bucketID)
			require.Equal(t, int64(1), d1.TokensRemaining(), "bucket %d per-minute should have 1 token", bucketID)
			require.Equal(t, perMinute, d1.Limit(), "bucket %d should have per-minute limit", bucketID)
			require.Equal(t, executionTime.ToTime(), d1.ExecutionTime(), "bucket %d execution time should match", bucketID)
			require.Equal(t, bucketID, d1.Input(), "bucket %d input should match", bucketID)
			require.Equal(t, expectedKey, d1.Key(), "bucket %d key should match", bucketID)
			require.Equal(t, int64(1), d1.TokensRequested(), "bucket %d should request 1 token", bucketID)
			require.Equal(t, int64(0), d1.TokensConsumed(), "bucket %d should consume 0 tokens when overall request is denied", bucketID)
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "bucket %d RetryAfter should be 0 when tokens are available", bucketID)
		}
	}

	// Refill per-second limit by advancing time
	executionTime = executionTime.Add(time.Second)

	// Per-second bucket should now have 1 more token available
	{
		results := make([][]Debug[int, string], buckets)

		var wg sync.WaitGroup
		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed, debugs := limiter.allowWithDebug(bucketID, executionTime)
				require.True(t, allowed, "bucket %d should allow request after per-second refill", bucketID)
				require.Len(t, debugs, 2, "should have debugs for both limits")
				results[bucketID] = debugs
			}(bucketID)
		}
		wg.Wait()

		// Verify all results show successful consumption
		for bucketID, debugs := range results {
			expectedKey := fmt.Sprintf("test-bucket-%d", bucketID)

			d0 := debugs[0]
			require.True(t, d0.Allowed(), "bucket %d per-second should allow", bucketID)
			require.Equal(t, perSecond.Count()-1, d0.TokensRemaining(), "bucket %d per-second should have 1 token remaining", bucketID)
			require.Equal(t, perSecond, d0.Limit(), "bucket %d should have per-second limit", bucketID)
			require.Equal(t, executionTime.ToTime(), d0.ExecutionTime(), "bucket %d execution time should match", bucketID)
			require.Equal(t, bucketID, d0.Input(), "bucket %d input should match", bucketID)
			require.Equal(t, expectedKey, d0.Key(), "bucket %d key should match", bucketID)
			require.Equal(t, int64(1), d0.TokensRequested(), "bucket %d should request 1 token", bucketID)
			require.Equal(t, int64(1), d0.TokensConsumed(), "bucket %d should consume 1 token when allowed", bucketID)
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "bucket %d RetryAfter should be 0 when allowed", bucketID)

			d1 := debugs[1]
			require.True(t, d1.Allowed(), "bucket %d per-minute should allow", bucketID)
			require.Equal(t, int64(0), d1.TokensRemaining(), "bucket %d per-minute should have 0 tokens remaining", bucketID)
			require.Equal(t, perMinute, d1.Limit(), "bucket %d should have per-minute limit", bucketID)
			require.Equal(t, executionTime.ToTime(), d1.ExecutionTime(), "bucket %d execution time should match", bucketID)
			require.Equal(t, bucketID, d1.Input(), "bucket %d input should match", bucketID)
			require.Equal(t, expectedKey, d1.Key(), "bucket %d key should match", bucketID)
			require.Equal(t, int64(1), d1.TokensRequested(), "bucket %d should request 1 token", bucketID)
			require.Equal(t, int64(1), d1.TokensConsumed(), "bucket %d should consume 1 token when allowed", bucketID)
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "bucket %d RetryAfter should be 0 when allowed", bucketID)
		}
	}

	// Now all buckets should be exhausted on per-minute limit
	{
		results := make([][]Debug[int, string], buckets)

		var wg sync.WaitGroup
		for bucketID := range buckets {
			wg.Add(1)
			go func(bucketID int) {
				defer wg.Done()
				allowed, debugs := limiter.allowWithDebug(bucketID, executionTime)
				require.False(t, allowed, "bucket %d should be exhausted on per-minute limit", bucketID)
				require.Len(t, debugs, 2, "should have debugs for both limits")
				results[bucketID] = debugs
			}(bucketID)
		}
		wg.Wait()

		// Verify all results show per-minute exhaustion
		for bucketID, debugs := range results {
			expectedKey := fmt.Sprintf("test-bucket-%d", bucketID)

			// per-second should still allow
			d0 := debugs[0]
			require.True(t, d0.Allowed(), "bucket %d per-second should still allow", bucketID)
			require.Equal(t, perSecond.Count()-1, d0.TokensRemaining(), "bucket %d per-second should have 1 token remaining", bucketID)
			require.Equal(t, perSecond, d0.Limit(), "bucket %d should have per-second limit", bucketID)
			require.Equal(t, executionTime.ToTime(), d0.ExecutionTime(), "bucket %d execution time should match", bucketID)
			require.Equal(t, bucketID, d0.Input(), "bucket %d input should match", bucketID)
			require.Equal(t, expectedKey, d0.Key(), "bucket %d key should match", bucketID)
			require.Equal(t, int64(1), d0.TokensRequested(), "bucket %d should request 1 token", bucketID)
			require.Equal(t, int64(0), d0.TokensConsumed(), "bucket %d should consume 0 tokens when overall request is denied", bucketID)
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "bucket %d per-second RetryAfter should be 0 when tokens are available", bucketID)

			// per-minute should be exhausted
			d1 := debugs[1]
			require.False(t, d1.Allowed(), "bucket %d per-minute should be denied", bucketID)
			require.Equal(t, int64(0), d1.TokensRemaining(), "bucket %d per-minute should have 0 tokens", bucketID)
			require.Equal(t, perMinute, d1.Limit(), "bucket %d should have per-minute limit", bucketID)
			require.Equal(t, executionTime.ToTime(), d1.ExecutionTime(), "bucket %d execution time should match", bucketID)
			require.Equal(t, bucketID, d1.Input(), "bucket %d input should match", bucketID)
			require.Equal(t, expectedKey, d1.Key(), "bucket %d key should match", bucketID)
			require.Equal(t, int64(1), d1.TokensRequested(), "bucket %d should request 1 token", bucketID)
			require.Equal(t, int64(0), d1.TokensConsumed(), "bucket %d should consume 0 tokens when denied", bucketID)
			// we moved forward by one second above, so the per-minute limit should be
			// 20s - 1s = 19s from its next token
			require.Equal(t, perMinute.durationPerToken-time.Second, d1.RetryAfter(), "bucket %d per-minute RetryAfter should be time to refill 1 token", bucketID)
		}
	}

	// Complete refill by advancing to refill both limits
	executionTime = executionTime.Add(time.Minute)

	// Test concurrent access after full refill
	{
		var wg sync.WaitGroup
		results := make([][]Debug[int, string], buckets*int(perSecond.count))
		resultIndex := 0

		for bucketID := range buckets {
			for processID := range perSecond.count {
				wg.Add(1)
				go func(bucketID int, processID int64, index int) {
					defer wg.Done()
					allowed, debugs := limiter.allowWithDebug(bucketID, executionTime)
					require.True(t, allowed, "process %d for bucket %d should be allowed after refill", processID, bucketID)
					require.Len(t, debugs, 2, "should have debugs for both limits")
					results[index] = debugs
				}(bucketID, processID, resultIndex)
				resultIndex++
			}
		}
		wg.Wait()

		// Verify all results after refill
		for i, debugs := range results {
			bucketID := i / int(perSecond.count) // Calculate bucketID from index
			expectedKey := fmt.Sprintf("test-bucket-%d", bucketID)

			d0 := debugs[0]
			require.True(t, d0.Allowed(), "debug %d per-second should be allowed", i)
			require.Equal(t, perSecond, d0.Limit(), "debug %d should have per-second limit", i)
			require.Equal(t, executionTime.ToTime(), d0.ExecutionTime(), "debug %d execution time should match", i)
			require.Equal(t, bucketID, d0.Input(), "debug %d input should match", i)
			require.Equal(t, expectedKey, d0.Key(), "debug %d key should match", i)
			require.Equal(t, int64(1), d0.TokensRequested(), "debug %d should request 1 token", i)
			require.Equal(t, int64(1), d0.TokensConsumed(), "debug %d should consume 1 token when allowed", i)
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "debug %d RetryAfter should be 0 when allowed", i)

			d1 := debugs[1]
			require.True(t, d1.Allowed(), "debug %d per-minute should be allowed", i)
			require.Equal(t, perMinute, d1.Limit(), "debug %d should have per-minute limit", i)
			require.Equal(t, executionTime.ToTime(), d1.ExecutionTime(), "debug %d execution time should match", i)
			require.Equal(t, bucketID, d1.Input(), "debug %d input should match", i)
			require.Equal(t, expectedKey, d1.Key(), "debug %d key should match", i)
			require.Equal(t, int64(1), d1.TokensRequested(), "debug %d should request 1 token", i)
			require.Equal(t, int64(1), d1.TokensConsumed(), "debug %d should consume 1 token when allowed", i)
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "debug %d RetryAfter should be 0 when allowed", i)
		}
	}
}

func TestLimiter_AllowWithDebug(t *testing.T) {
	t.Parallel()

	t.Run("SingleBucket", func(t *testing.T) {
		t.Parallel()

		t.Run("MultipleLimits", func(t *testing.T) {
			t.Parallel()
			keyFunc := func(input string) string {
				return input
			}
			perSecond := NewLimit(2, time.Second)
			perMinute := NewLimit(3, time.Minute)
			limiter := NewLimiter(keyFunc, perSecond, perMinute)

			const bucketID = "test-allow-with-debug"

			executionTime := ntime.Now()
			{
				// exhaust the per-second limit, but per-minute should still have 1 token
				for i := range perSecond.Count() {
					allowed, debugs := limiter.allowWithDebug(bucketID, executionTime)
					require.True(t, allowed)
					require.Len(t, debugs, 2, "should have debugs for both limits")

					d0 := debugs[0]
					require.Equal(t, perSecond, d0.Limit(), "should have per-second limit in debug")
					require.Equal(t, allowed, d0.Allowed(), "allowed should match for per-second limit")
					require.Equal(t, executionTime.ToTime(), d0.ExecutionTime(), "execution time should match for per-second limit")
					require.Equal(t, bucketID, d0.Input(), "input should match for per-second limit")
					require.Equal(t, bucketID, d0.Key(), "bucket key should match for per-second limit")
					require.Equal(t, int64(1), d0.TokensRequested(), "per-second limit should request 1 token")
					require.Equal(t, int64(1), d0.TokensConsumed(), "per-second limit should consume 1 token when allowed")
					require.Equal(t, perSecond.Count()-i-1, d0.TokensRemaining(), "remaining tokens should match for per-second limit")
					require.Equal(t, time.Duration(0), d0.RetryAfter(), "per-second RetryAfter should be 0 when allowed")

					d1 := debugs[1]
					require.Equal(t, perMinute, d1.Limit(), "should have per-minute limit in debug")
					require.Equal(t, allowed, d1.Allowed(), "allowed should match for per-minute limit")
					require.Equal(t, executionTime.ToTime(), d1.ExecutionTime(), "execution time should match for per-minute limit")
					require.Equal(t, bucketID, d1.Input(), "input should match for per-minute limit")
					require.Equal(t, bucketID, d1.Key(), "bucket key should match for per-minute limit")
					require.Equal(t, int64(1), d1.TokensRequested(), "per-minute limit should request 1 token")
					require.Equal(t, int64(1), d1.TokensConsumed(), "per-minute limit should consume 1 token when allowed")
					require.Equal(t, perMinute.Count()-i-1, d1.TokensRemaining(), "remaining tokens should match for per-minute limit")
					require.Equal(t, time.Duration(0), d1.RetryAfter(), "per-minute RetryAfter should be 0 when allowed")
				}
			}

			{
				allowed, debugs := limiter.allowWithDebug(bucketID, executionTime)
				require.False(t, allowed)
				require.Len(t, debugs, 2, "should have debugs for both limits")

				// per-second should have been denied
				d0 := debugs[0]
				require.False(t, d0.Allowed(), "allowed should match for per-second limit")
				require.Equal(t, int64(0), d0.TokensRemaining(), "remaining tokens should match for per-second limit")
				require.Equal(t, perSecond, d0.Limit(), "should have per-second limit in debug")
				require.Equal(t, executionTime.ToTime(), d0.ExecutionTime(), "execution time should match for per-second limit")
				require.Equal(t, bucketID, d0.Input(), "input should match for per-second limit")
				require.Equal(t, bucketID, d0.Key(), "bucket key should match for per-second limit")
				require.Equal(t, int64(1), d0.TokensRequested(), "per-second limit should request 1 token")
				require.Equal(t, int64(0), d0.TokensConsumed(), "per-second limit should consume 0 tokens when denied")
				// per-second limit is exhausted, so we need to wait for 1 token to refill
				expectedRetryAfter := perSecond.durationPerToken
				require.Equal(t, expectedRetryAfter, d0.RetryAfter(), "per-second RetryAfter should be time to refill 1 token")

				// per-minute still has 1 token
				d1 := debugs[1]
				require.True(t, d1.Allowed(), "allowed should match for per-minute limit")
				require.Equal(t, int64(1), d1.TokensRemaining(), "per-minute limit should have 1 remaining token")
				require.Equal(t, perMinute, d1.Limit(), "should have per-minute limit in debug")
				require.Equal(t, executionTime.ToTime(), d1.ExecutionTime(), "execution time should match for per-minute limit")
				require.Equal(t, bucketID, d1.Input(), "input should match for per-minute limit")
				require.Equal(t, bucketID, d1.Key(), "bucket key should match for per-minute limit")
				require.Equal(t, int64(1), d1.TokensRequested(), "per-minute limit should request 1 token")
				require.Equal(t, int64(0), d1.TokensConsumed(), "per-minute limit should consume 0 tokens when overall request is denied")
				require.Equal(t, time.Duration(0), d1.RetryAfter(), "per-minute RetryAfter should be 0 when tokens are available")
			}
		})
	})

	t.Run("Func", func(t *testing.T) {
		t.Parallel()
		keyFunc := func(input string) string {
			return input
		}
		limit := NewLimit(11, time.Second)
		limitFunc := func(input string) Limit { return limit }
		limiter := NewLimiterFunc(keyFunc, limitFunc)

		now := ntime.Now()

		allow, details := limiter.allowWithDebug("test-details", now)
		require.True(t, allow)
		d := details[0]
		require.True(t, d.Allowed(), "should be allowed")
		require.Equal(t, limit, d.Limit())
		require.Equal(t, now.ToTime(), d.ExecutionTime())
		require.Equal(t, "test-details", d.Input(), "input should match")
		require.Equal(t, "test-details", d.Key())
		require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(1), d.TokensConsumed(), "should consume 1 token when allowed")
		require.Equal(t, limit.count-1, d.TokensRemaining())
		require.Equal(t, time.Duration(0), d.RetryAfter(), "RetryAfter should be 0 when allowed")
	})

	t.Run("TokensRequestedAndConsumed", func(t *testing.T) {
		t.Parallel()

		keyFunc := func(input string) string { return input }
		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)

		// Test AllowWithDetails when request is allowed
		t.Run("AllowWithDetails_Allowed", func(t *testing.T) {
			allowed, details := limiter.AllowWithDebug("test-key1")
			require.True(t, allowed, "request should be allowed")
			require.Len(t, details, 1, "should have one detail")

			d := details[0]
			require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
			require.Equal(t, int64(1), d.TokensConsumed(), "should consume 1 token when allowed")
			require.Equal(t, int64(4), d.TokensRemaining(), "should have 4 tokens remaining")
		})

		// Test PeekWithDetails (no consumption)
		t.Run("PeekWithDetails", func(t *testing.T) {
			allowed, details := limiter.PeekWithDebug("test-key2")
			require.True(t, allowed, "peek should show available tokens")
			require.Len(t, details, 1, "should have one detail")

			d := details[0]
			require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
			require.Equal(t, int64(0), d.TokensConsumed(), "peek should not consume tokens")
			require.Equal(t, int64(5), d.TokensRemaining(), "should have all 5 tokens remaining")
		})

		// Test AllowWithDetails when request is denied
		t.Run("AllowWithDetails_Denied", func(t *testing.T) {
			// Exhaust the bucket
			for range limit.count {
				limiter.Allow("test-key3")
			}

			allowed, details := limiter.AllowWithDebug("test-key3")
			require.False(t, allowed, "request should be denied")
			require.Len(t, details, 1, "should have one detail")

			d := details[0]
			require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
			require.Equal(t, int64(0), d.TokensConsumed(), "should consume 0 tokens when denied")
			require.Equal(t, int64(0), d.TokensRemaining(), "should have 0 tokens remaining")
		})
	})
}

func TestLimiter_UsesLimitFunc(t *testing.T) {
	t.Parallel()

	t.Run("DynamicLimits", func(t *testing.T) {
		t.Parallel()
		keyFunc := func(input int) string {
			return fmt.Sprintf("test-bucket-%d", input)
		}
		{
			limitFunc := func(input int) Limit {
				return NewLimit(int64(10*input), time.Second)
			}
			limiter := NewLimiterFunc(keyFunc, limitFunc)

			for i := range 3 {
				allow, details := limiter.allowWithDebug(i+1, ntime.Now())
				require.True(t, allow)
				d := details[0]
				require.Equal(t, limitFunc(i+1), d.Limit())
			}
		}
		{
			limit := NewLimit(888, time.Second)
			limiter := NewLimiter(keyFunc, limit)

			for i := range 3 {
				allow, details := limiter.allowWithDebug(i+1, ntime.Now())
				require.True(t, allow)
				d := details[0]
				require.Equal(t, limit, d.Limit())
			}
		}
	})
}

func TestLimiter_AllowWithDetails(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string { return input }

	// Test single limit
	t.Run("SingleLimit", func(t *testing.T) {
		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)

		// Test when request is allowed
		allowed, details := limiter.AllowWithDetails("test-key1")
		require.True(t, allowed, "request should be allowed")
		require.Equal(t, true, details.Allowed(), "should be allowed")
		require.Equal(t, int64(1), details.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(1), details.TokensConsumed(), "should consume 1 token when allowed")
		require.Equal(t, int64(4), details.TokensRemaining(), "should have 4 tokens remaining")
		require.GreaterOrEqual(t, details.RetryAfter(), time.Duration(0), "retry after should be non-negative")
	})

	// Test multiple limits - this is the key scenario
	t.Run("MultipleLimits", func(t *testing.T) {
		perSecond := NewLimit(2, time.Second) // 2 per second = 500ms per token
		perMinute := NewLimit(3, time.Minute) // 3 per minute = 20s per token
		limiter := NewLimiter(keyFunc, perSecond, perMinute)

		// Allow first request
		allowed, details := limiter.AllowWithDetails("test-multi")
		require.True(t, allowed, "first request should be allowed")
		require.Equal(t, int64(1), details.TokensRequested())
		require.Equal(t, int64(1), details.TokensConsumed())

		// Remaining tokens should be minimum across buckets: min(1, 2) = 1
		require.Equal(t, int64(1), details.TokensRemaining(), "should have min remaining tokens across buckets")

		// Allow second request
		allowed, details = limiter.AllowWithDetails("test-multi")
		require.True(t, allowed, "second request should be allowed")

		// Remaining tokens should be minimum across buckets: min(0, 1) = 0
		require.Equal(t, int64(0), details.TokensRemaining(), "should have min remaining tokens across buckets")

		// Try third request - should be denied because per-second bucket is exhausted
		allowed, details = limiter.AllowWithDetails("test-multi")
		require.False(t, allowed, "third request should be denied")
		require.Equal(t, int64(0), details.TokensConsumed(), "should consume 0 tokens when denied")
		require.Equal(t, int64(0), details.TokensRemaining(), "should show 0 remaining when denied")

		// RetryAfter should be time needed for per-second bucket to refill (since it's the bottleneck)
		require.Greater(t, details.RetryAfter(), time.Duration(0), "retry after should be positive when denied")
		require.LessOrEqual(t, details.RetryAfter(), perSecond.DurationPerToken(), "retry after should not exceed per-token duration")
	})

	// Test RetryAfter with multiple limits - comprehensive scenarios
	t.Run("MultipleLimits_RetryAfter", func(t *testing.T) {
		// Create limits with different refill rates to test RetryAfter logic
		fast := NewLimit(10, time.Second) // 10 per second = 100ms per token
		slow := NewLimit(6, time.Minute)  // 6 per minute = 10s per token
		limiter := NewLimiter(keyFunc, fast, slow)

		baseTime := ntime.Now()

		// Test when both buckets are exhausted - RetryAfter should be max across buckets
		t.Run("BothBucketsExhausted", func(t *testing.T) {
			// Exhaust both buckets completely
			for i := range 10 { // Exhaust fast bucket (10 tokens)
				allowed := limiter.allowN("test-both-exhausted", baseTime, 1)
				if i < 6 { // First 6 requests should succeed (limited by slow bucket)
					require.True(t, allowed, "request %d should be allowed", i)
				} else { // Requests 7-10 should fail (slow bucket exhausted)
					require.False(t, allowed, "request %d should be denied", i)
				}
			}

			// Now both buckets should be exhausted, test RetryAfter
			allowed, details := limiter.allowNWithDetails("test-both-exhausted", baseTime, 1)
			require.False(t, allowed, "request should be denied when both buckets exhausted")

			// RetryAfter should be the MAX time needed across buckets
			// Fast bucket needs 100ms for next token, slow bucket needs 10s
			// So RetryAfter should be ~10s (the slower one)
			expectedSlowRetry := slow.DurationPerToken() // 10s
			require.GreaterOrEqual(t, details.RetryAfter(), expectedSlowRetry-time.Millisecond,
				"retry after should be at least slow bucket duration")
			require.LessOrEqual(t, details.RetryAfter(), expectedSlowRetry+time.Millisecond,
				"retry after should not exceed slow bucket duration by much")
		})

		// Test when only fast bucket is exhausted
		t.Run("FastBucketExhausted", func(t *testing.T) {
			// Allow some requests to partially exhaust fast bucket but not slow bucket
			for i := range 3 { // Use 3 tokens, leaving fast=7, slow=3
				allowed := limiter.allowN("test-fast-exhausted", baseTime, 1)
				require.True(t, allowed, "request %d should be allowed", i)
			}

			// Exhaust remaining fast bucket tokens (7 more requests)
			for i := range 7 {
				allowed := limiter.allowN("test-fast-exhausted", baseTime, 1)
				if i < 3 { // Requests 4-6 should succeed (slow bucket still has tokens)
					require.True(t, allowed, "request %d should be allowed", i+3)
				} else { // Requests 7+ should fail (slow bucket now exhausted)
					require.False(t, allowed, "request %d should be denied", i+3)
				}
			}

			// At this point: fast bucket exhausted (0 tokens), slow bucket exhausted (0 tokens)
			// Test RetryAfter - should still be dominated by slow bucket
			allowed, details := limiter.allowNWithDetails("test-fast-exhausted", baseTime, 1)
			require.False(t, allowed, "request should be denied")

			// RetryAfter should be max(fast_retry, slow_retry) = max(100ms, 10s) = 10s
			expectedRetry := slow.DurationPerToken() // 10s (slower bucket)
			require.GreaterOrEqual(t, details.RetryAfter(), expectedRetry-time.Millisecond,
				"retry after should be dominated by slower bucket")
		})

		// Test requesting multiple tokens - RetryAfter should account for N tokens
		t.Run("MultipleTokensRequest", func(t *testing.T) {
			// Request 3 tokens when buckets are fresh
			allowed, details := limiter.allowNWithDetails("test-multi-tokens", baseTime, 3)
			require.True(t, allowed, "request for 3 tokens should be allowed on fresh buckets")
			require.Equal(t, int64(3), details.TokensConsumed())

			// Remaining: fast=7, slow=3
			// Request 4 more tokens - should be denied because slow bucket only has 3
			allowed, details = limiter.allowNWithDetails("test-multi-tokens", baseTime, 4)
			require.False(t, allowed, "request for 4 tokens should be denied")
			require.Equal(t, int64(0), details.TokensConsumed())

			// RetryAfter should be time needed for slow bucket to accumulate 4 tokens
			// Since slow bucket has 3 tokens, it needs 1 more token = 10s
			expectedRetry := slow.DurationPerToken() * 1 // 10s for 1 additional token
			require.GreaterOrEqual(t, details.RetryAfter(), expectedRetry-time.Millisecond,
				"retry after should account for tokens needed in limiting bucket")
			require.LessOrEqual(t, details.RetryAfter(), expectedRetry+time.Millisecond,
				"retry after should not exceed expected duration by much")
		})

		// Test when request is allowed - RetryAfter should be 0
		t.Run("AllowedRequest", func(t *testing.T) {
			// Fresh limiter, request should be allowed
			freshLimiter := NewLimiter(keyFunc, fast, slow)
			allowed, details := freshLimiter.allowNWithDetails("test-allowed", baseTime, 1)
			require.True(t, allowed, "request should be allowed on fresh limiter")
			require.Equal(t, time.Duration(0), details.RetryAfter(),
				"retry after should be 0 when request is allowed")
		})

		// Test edge case: different buckets have different next-token times
		t.Run("DifferentNextTokenTimes", func(t *testing.T) {
			// Create a scenario where buckets have different "next token" times
			// Use a custom time that progresses through the test
			testTime := baseTime

			// Consume tokens to create different states in each bucket
			// The limiter has fast (10/sec) and slow (6/min) limits
			// So we can consume at most 6 tokens total (limited by slow bucket)
			for i := range 6 {
				allowed := limiter.allowN("test-different-times", testTime, 1)
				require.True(t, allowed, "request %d should be allowed", i)
			}
			// After 6 requests:
			// Fast bucket: 10-6=4 tokens remaining
			// Slow bucket: 6-6=0 tokens remaining (exhausted)

			// Try 7th request - should fail because slow bucket is exhausted
			allowed := limiter.allowN("test-different-times", testTime, 1)
			require.False(t, allowed, "7th request should be denied due to slow bucket exhaustion")

			// Move time forward by 50ms (half of fast bucket's refill time)
			testTime = testTime.Add(50 * time.Millisecond)

			// Request 1 token - should still be denied because slow bucket needs 10s to refill
			allowed, details := limiter.allowNWithDetails("test-different-times", testTime, 1)
			require.False(t, allowed, "request should still be denied due to slow bucket")

			// RetryAfter should be dominated by slow bucket since it needs 10s for next token
			// Fast bucket would only need ~50ms more, but slow bucket needs ~9.95s more
			expectedRetry := slow.DurationPerToken() - 50*time.Millisecond // ~9.95s
			require.GreaterOrEqual(t, details.RetryAfter(), expectedRetry-100*time.Millisecond,
				"retry after should be dominated by slow bucket needing ~9.95s")
			require.LessOrEqual(t, details.RetryAfter(), expectedRetry+100*time.Millisecond,
				"retry after should not exceed expected time by much")
		})
	})

	// Test PeekWithDetails doesn't consume tokens
	t.Run("PeekDoesNotConsume", func(t *testing.T) {
		limit := NewLimit(2, time.Second)
		limiter := NewLimiter(keyFunc, limit)

		// Peek should not consume
		allowed, details := limiter.PeekWithDetails("test-peek")
		require.True(t, allowed, "peek should show available tokens")
		require.Equal(t, int64(1), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed(), "peek should not consume tokens")
		require.Equal(t, int64(2), details.TokensRemaining(), "should have all tokens remaining")

		// Actual allow should consume
		allowed, details = limiter.AllowWithDetails("test-peek")
		require.True(t, allowed, "allow should succeed")
		require.Equal(t, int64(1), details.TokensConsumed(), "allow should consume tokens")
		require.Equal(t, int64(1), details.TokensRemaining(), "should have 1 token remaining after consumption")
	})

	// Test edge cases and potential bugs
	t.Run("EdgeCases", func(t *testing.T) {
		// Test zero limits (no limits defined)
		t.Run("NoLimits", func(t *testing.T) {
			limiter := NewLimiter(keyFunc) // No limits
			allowed, details := limiter.AllowWithDetails("test-no-limits")
			require.True(t, allowed, "should allow when no limits defined")
			require.Equal(t, int64(1), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed(), "BUG: should be 0 consumed when no limits, but implementation says 0")
			require.Equal(t, int64(0), details.TokensRemaining(), "should show 0 remaining when no limits")
			require.Equal(t, time.Duration(0), details.RetryAfter(), "should show 0 retry after when no limits")
		})

		// Test requesting zero tokens
		t.Run("ZeroTokensRequest", func(t *testing.T) {
			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			allowed, details := limiter.AllowNWithDetails("test-zero", 0)
			require.True(t, allowed, "requesting 0 tokens should always be allowed")
			require.Equal(t, int64(0), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(5), details.TokensRemaining(), "should not affect remaining tokens")
			require.Equal(t, time.Duration(0), details.RetryAfter())
		})

		// Test negative tokens request (potential edge case)
		t.Run("NegativeTokensRequest", func(t *testing.T) {
			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			// This might cause undefined behavior - let's see what happens
			allowed, details := limiter.AllowNWithDetails("test-negative", -1)
			// Behavior is undefined, but we should document what actually happens
			t.Logf("Negative tokens request: allowed=%v, consumed=%d, remaining=%d",
				allowed, details.TokensConsumed(), details.TokensRemaining())
		})

		// Test very large token request
		t.Run("LargeTokensRequest", func(t *testing.T) {
			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			allowed, details := limiter.AllowNWithDetails("test-large", 1000000)
			require.False(t, allowed, "requesting huge number of tokens should be denied")
			require.Equal(t, int64(1000000), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(5), details.TokensRemaining())
			// RetryAfter should be very large for 1M tokens
			expectedRetry := limit.DurationPerToken() * 1000000
			require.GreaterOrEqual(t, details.RetryAfter(), expectedRetry-time.Second)
		})

		// Test potential race condition in remaining tokens calculation
		t.Run("RemainingTokensAfterConsumption", func(t *testing.T) {
			// This tests a potential bug: are remaining tokens calculated correctly
			// after consumption in the multiple-limits case?
			fast := NewLimit(10, time.Second) // 100ms per token
			slow := NewLimit(5, time.Minute)  // 12s per token
			limiter := NewLimiter(keyFunc, fast, slow)

			baseTime := ntime.Now()

			// Consume 3 tokens
			allowed, details := limiter.allowNWithDetails("test-remaining", baseTime, 3)
			require.True(t, allowed, "should allow 3 tokens initially")
			require.Equal(t, int64(3), details.TokensConsumed())

			// After consumption: fast bucket has 7, slow bucket has 2
			// Remaining should be min(7, 2) = 2
			require.Equal(t, int64(2), details.TokensRemaining(),
				"remaining tokens should be min across buckets after consumption")

			// Consume 2 more tokens
			allowed, details = limiter.allowNWithDetails("test-remaining", baseTime, 2)
			require.True(t, allowed, "should allow 2 more tokens")

			// After consumption: fast bucket has 5, slow bucket has 0
			// Remaining should be min(5, 0) = 0
			require.Equal(t, int64(0), details.TokensRemaining(),
				"remaining tokens should be 0 when any bucket is exhausted")
		})

		// Test bucket cutoff behavior
		t.Run("BucketCutoffBehavior", func(t *testing.T) {
			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)

			// Start at a base time
			oldTime := ntime.Now()

			// Use up some tokens at old time
			allowed := limiter.allowN("test-cutoff", oldTime, 3)
			require.True(t, allowed, "should allow tokens at old time")

			// Jump forward way past the bucket period (more than 1 second)
			newTime := oldTime.Add(10 * time.Second)

			// Now check details - bucket should be reset due to cutoff
			allowed, details := limiter.allowNWithDetails("test-cutoff", newTime, 1)
			require.True(t, allowed, "should allow after cutoff reset")

			// After cutoff, bucket should have full tokens available
			// But we consumed 1, so should have 4 remaining
			require.Equal(t, int64(4), details.TokensRemaining(),
				"should have nearly full tokens after cutoff reset")
		})
	})
}
