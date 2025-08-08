package rate

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiters_AllowN_SingleLimiter(t *testing.T) {
	t.Parallel()

	keyer := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("SingleLimit", func(t *testing.T) {
		t.Parallel()

		t.Run("Serial", func(t *testing.T) {
			t.Parallel()

			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyer, limit)

			limiters := NewLimiters(limiter)

			now := time.Now()

			// Should allow the first 5 requests
			for i := range 5 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "request %d should be allowed", i+1)
			}

			// The 6th request should be denied (bucket exhausted)
			allowed := limiters.allowN("test", now, 1)
			require.False(t, allowed, "6th request should be denied")

			// After advancing time by one token duration, should allow one more request
			tokenDuration := limit.durationPerToken
			futureTime := now.Add(tokenDuration)
			allowed = limiters.allowN("test", futureTime, 1)
			require.True(t, allowed, "request after token refill should be allowed")
		})

		t.Run("Concurrent", func(t *testing.T) {
			t.Parallel()

			limit := NewLimit(10, time.Second)
			limiter := NewLimiter(keyer, limit)

			limiters := NewLimiters(limiter)

			now := time.Now()
			const concurrency = 20

			// Run 20 goroutines concurrently, expecting only 10 to succeed
			results := make([]bool, concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index] = limiters.allowN("test", now, 1)
				}(i)
			}
			wg.Wait()

			successes := 0
			for _, result := range results {
				if result {
					successes++
				}
			}

			// Should allow exactly 10 requests (the limit)
			require.Equal(t, 10, successes, "should allow exactly 10 concurrent requests")
		})
	})

	t.Run("MultipleLimits", func(t *testing.T) {
		t.Parallel()

		t.Run("Serial", func(t *testing.T) {
			t.Parallel()

			limit1 := NewLimit(10, time.Second)
			limit2 := NewLimit(50, time.Minute)
			limiter := NewLimiter(keyer, limit1, limit2)

			limiters := NewLimiters(limiter)

			now := time.Now()

			// Should allow the first 10 requests (limited by per-second limit)
			for i := range 10 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "request %d should be allowed", i+1)
			}

			// The 11th request should be denied (per-second bucket exhausted)
			allowed := limiters.allowN("test", now, 1)
			require.False(t, allowed, "11th request should be denied")

			// After advancing time by one second, should refill per-second bucket
			futureTime := now.Add(time.Second)

			// Should allow 10 more requests
			for i := range 10 {
				allowed := limiters.allowN("test", futureTime, 1)
				require.True(t, allowed, "request %d after refill should be allowed", i+1)
			}

			// Now we've consumed 20 requests total, should still be under the 50/minute limit
			allowed = limiters.allowN("test", futureTime, 1)
			require.False(t, allowed, "should be denied due to per-second limit, not per-minute")

			// Test consuming multiple tokens across both limits
			evenLaterTime := futureTime.Add(time.Second)
			allowed = limiters.allowN("test", evenLaterTime, 5)
			require.True(t, allowed, "should allow consuming 5 tokens across both limits")

			// Should have 5 tokens remaining in per-second bucket
			allowed = limiters.allowN("test", evenLaterTime, 5)
			require.True(t, allowed, "should allow consuming remaining 5 tokens")

			// Should deny further requests until next refill
			allowed = limiters.allowN("test", evenLaterTime, 1)
			require.False(t, allowed, "should deny when per-second bucket exhausted")
		})

		t.Run("Concurrent", func(t *testing.T) {
			t.Parallel()

			limit1 := NewLimit(8, time.Second)  // 8 requests per second (more restrictive)
			limit2 := NewLimit(30, time.Minute) // 30 requests per minute
			limiter := NewLimiter(keyer, limit1, limit2)

			limiters := NewLimiters(limiter)

			now := time.Now()
			const concurrency = 20

			// Run 20 goroutines concurrently, expecting only 8 to succeed (limited by per-second)
			results := make([]bool, concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index] = limiters.allowN("test", now, 1)
				}(i)
			}
			wg.Wait()

			// Count successful requests
			successes := 0
			for _, result := range results {
				if result {
					successes++
				}
			}

			// Should allow exactly 8 requests (limited by the more restrictive per-second limit)
			require.Equal(t, 8, successes, "should allow exactly 8 concurrent requests due to per-second limit")

			// Test concurrent access after refill
			futureTime := now.Add(time.Second)
			results2 := make([]bool, concurrency)

			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index] = limiters.allowN("test", futureTime, 1)
				}(i)
			}

			wg.Wait()

			// Count successful requests after refill
			successCount2 := 0
			for _, result := range results2 {
				if result {
					successCount2++
				}
			}

			// Should allow exactly 8 more requests after refill
			require.Equal(t, 8, successCount2, "should allow exactly 8 concurrent requests after refill")
		})
	})
}

func TestLimiters_AllowN_MultipleLimiters(t *testing.T) {
	t.Parallel()

	keyer := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("SameKeyer", func(t *testing.T) {
		t.Parallel()

		t.Run("SingleLimits", func(t *testing.T) {
			t.Parallel()

			t.Run("Serial", func(t *testing.T) {
				t.Parallel()

				limit1 := NewLimit(3, time.Second)
				limit2 := NewLimit(5, time.Second)
				limiter1 := NewLimiter(keyer, limit1)
				limiter2 := NewLimiter(keyer, limit2)

				// Create Limiters with both limiters
				limiters := NewLimiters(limiter1, limiter2)

				now := time.Now()

				// Should allow the first 3 requests (limited by the more restrictive limiter1)
				for i := range 3 {
					allowed := limiters.allowN("test", now, 1)
					require.True(t, allowed, "request %d should be allowed", i+1)
				}

				// The 4th request should be denied (limiter1 bucket exhausted)
				allowed := limiters.allowN("test", now, 1)
				require.False(t, allowed, "4th request should be denied due to limiter1 exhaustion")

				// After advancing time to refill limiter1, should allow more requests
				futureTime := now.Add(limit1.durationPerToken)
				allowed = limiters.allowN("test", futureTime, 1)
				require.True(t, allowed, "request after limiter1 refill should be allowed")
			})

			t.Run("Concurrent", func(t *testing.T) {
				t.Parallel()

				limit1 := NewLimit(3, time.Second) // 3 requests per second (most restrictive)
				limit2 := NewLimit(5, time.Second) // 5 requests per second
				limiter1 := NewLimiter(keyer, limit1)
				limiter2 := NewLimiter(keyer, limit2)

				// Create Limiters with both limiters
				limiters := NewLimiters(limiter1, limiter2)

				now := time.Now()
				const concurrency = 10

				// Run 10 goroutines concurrently, expecting only 3 to succeed (limited by limiter1)
				results := make([]bool, concurrency)

				var wg sync.WaitGroup
				for i := range concurrency {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						results[index] = limiters.allowN("test", now, 1)
					}(i)
				}
				wg.Wait()

				// Count successful requests
				successes := 0
				for _, result := range results {
					if result {
						successes++
					}
				}

				// Should allow exactly 3 requests (limited by the more restrictive limiter1)
				require.Equal(t, 3, successes, "should allow exactly 3 concurrent requests due to limiter1 restriction")
			})
		})

		t.Run("MultipleLimits", func(t *testing.T) {
			t.Parallel()

			t.Run("Serial", func(t *testing.T) {
				t.Parallel()

				limit1a := NewLimit(5, time.Second)
				limit1b := NewLimit(100, time.Minute)
				limiter1 := NewLimiter(keyer, limit1a, limit1b)

				limit2a := NewLimit(10, time.Second)
				limit2b := NewLimit(50, time.Minute)
				limiter2 := NewLimiter(keyer, limit2a, limit2b)

				limiters := NewLimiters(limiter1, limiter2)

				now := time.Now()

				// Should allow the first 5 requests (limited by limiter1's per-second limit)
				for i := range 5 {
					allowed := limiters.allowN("test", now, 1)
					require.True(t, allowed, "request %d should be allowed", i+1)
				}

				// The 6th request should be denied (limiter1's per-second bucket exhausted)
				allowed := limiters.allowN("test", now, 1)
				require.False(t, allowed, "6th request should be denied due to limiter1 per-second exhaustion")

				// After advancing time by one second, should refill per-second buckets
				futureTime := now.Add(time.Second)

				// Should allow 5 more requests (still limited by limiter1's per-second)
				for i := range 5 {
					allowed := limiters.allowN("test", futureTime, 1)
					require.True(t, allowed, "request %d after refill should be allowed", i+1)
				}

				// Now we've consumed 10 requests total, test multiple token consumption
				evenLaterTime := futureTime.Add(time.Second)
				allowed = limiters.allowN("test", evenLaterTime, 3)
				require.True(t, allowed, "should allow consuming 3 tokens across all limits")

				// Should have 2 tokens remaining in limiter1's per-second bucket
				allowed = limiters.allowN("test", evenLaterTime, 2)
				require.True(t, allowed, "should allow consuming remaining 2 tokens")

				// Should deny further requests
				allowed = limiters.allowN("test", evenLaterTime, 1)
				require.False(t, allowed, "should deny when limiter1 per-second bucket exhausted")
			})

			t.Run("Concurrent", func(t *testing.T) {
				t.Parallel()

				limit1a := NewLimit(4, time.Second)  // 4 requests per second (most restrictive)
				limit1b := NewLimit(60, time.Minute) // 60 requests per minute
				limiter1 := NewLimiter(keyer, limit1a, limit1b)

				limit2a := NewLimit(8, time.Second)  // 8 requests per second
				limit2b := NewLimit(40, time.Minute) // 40 requests per minute (more restrictive per-minute)
				limiter2 := NewLimiter(keyer, limit2a, limit2b)

				limiters := NewLimiters(limiter1, limiter2)

				now := time.Now()
				const concurrency = 15

				// Run 15 goroutines concurrently, expecting only 4 to succeed (limited by limiter1's per-second)
				results := make([]bool, concurrency)

				var wg sync.WaitGroup
				for i := range concurrency {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						results[index] = limiters.allowN("test", now, 1)
					}(i)
				}
				wg.Wait()

				// Count successful requests
				successes := 0
				for _, result := range results {
					if result {
						successes++
					}
				}

				// Should allow exactly 4 requests (limited by limiter1's per-second limit)
				require.Equal(t, 4, successes, "should allow exactly 4 concurrent requests due to limiter1 per-second restriction")

				// Test concurrent access after refill
				futureTime := now.Add(time.Second)
				results2 := make([]bool, concurrency)

				for i := range concurrency {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						results2[index] = limiters.allowN("test", futureTime, 1)
					}(i)
				}
				wg.Wait()

				// Count successful requests after refill
				successCount2 := 0
				for _, result := range results2 {
					if result {
						successCount2++
					}
				}

				// Should allow exactly 4 more requests after refill
				require.Equal(t, 4, successCount2, "should allow exactly 4 concurrent requests after refill")
			})
		})
	})

	t.Run("DifferentKeyers", func(t *testing.T) {
		t.Parallel()

		keyer1 := func(input string) string {
			return fmt.Sprintf("limiter1-%s", input)
		}
		keyer2 := func(input string) string {
			return fmt.Sprintf("limiter2-%s", input)
		}

		t.Run("SingleLimits", func(t *testing.T) {
			t.Parallel()

			t.Run("Serial", func(t *testing.T) {
				t.Parallel()

				limit1 := NewLimit(3, time.Second)
				limit2 := NewLimit(5, time.Second)
				limiter1 := NewLimiter(keyer1, limit1)
				limiter2 := NewLimiter(keyer2, limit2)

				// Create Limiters with both limiters
				limiters := NewLimiters(limiter1, limiter2)

				now := time.Now()

				// Should allow the first 3 requests (limited by the more restrictive limiter1)
				for i := range 3 {
					allowed := limiters.allowN("test", now, 1)
					require.True(t, allowed, "request %d should be allowed", i+1)
				}

				// The 4th request should be denied (limiter1 bucket exhausted)
				allowed := limiters.allowN("test", now, 1)
				require.False(t, allowed, "4th request should be denied due to limiter1 exhaustion")

				// Using a different input should work (different bucket keys)
				allowed = limiters.allowN("different", now, 1)
				require.True(t, allowed, "request with different input should be allowed")

				// Should still be limited by limiter1 for the different input
				for i := range 2 { // 2 more to reach limiter1's limit
					allowed = limiters.allowN("different", now, 1)
					require.True(t, allowed, "request %d for different input should be allowed", i+2)
				}

				// Should deny 4th request for different input too
				allowed = limiters.allowN("different", now, 1)
				require.False(t, allowed, "4th request for different input should be denied")

				// After advancing time to refill limiter1, should allow more requests
				futureTime := now.Add(limit1.durationPerToken)
				allowed = limiters.allowN("test", futureTime, 1)
				require.True(t, allowed, "request after limiter1 refill should be allowed")
			})

			t.Run("Concurrent", func(t *testing.T) {
				t.Parallel()

				limit1 := NewLimit(3, time.Second) // 3 requests per second (most restrictive)
				limit2 := NewLimit(5, time.Second) // 5 requests per second
				limiter1 := NewLimiter(keyer1, limit1)
				limiter2 := NewLimiter(keyer2, limit2)

				// Create Limiters with both limiters
				limiters := NewLimiters(limiter1, limiter2)

				now := time.Now()
				const concurrency = 10

				// Run 10 goroutines concurrently, expecting only 3 to succeed (limited by limiter1)
				results := make([]bool, concurrency)

				var wg sync.WaitGroup
				for i := range concurrency {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						results[index] = limiters.allowN("test", now, 1)
					}(i)
				}
				wg.Wait()

				// Count successful requests
				successes := 0
				for _, result := range results {
					if result {
						successes++
					}
				}

				// Should allow exactly 3 requests (limited by the more restrictive limiter1)
				require.Equal(t, 3, successes, "should allow exactly 3 concurrent requests due to limiter1 restriction")

				// Test with different input (different bucket keys) - should also be limited to 3
				results2 := make([]bool, concurrency)
				for i := range concurrency {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						results2[index] = limiters.allowN("different", now, 1)
					}(i)
				}
				wg.Wait()

				// Count successful requests for different input
				successCount2 := 0
				for _, result := range results2 {
					if result {
						successCount2++
					}
				}

				// Should allow exactly 3 requests for different input too
				require.Equal(t, 3, successCount2, "should allow exactly 3 concurrent requests for different input")
			})
		})

		t.Run("MultipleLimits", func(t *testing.T) {
			t.Parallel()

			t.Run("Serial", func(t *testing.T) {
				t.Parallel()

				limit1a := NewLimit(4, time.Second)
				limit1b := NewLimit(80, time.Minute)
				limiter1 := NewLimiter(keyer1, limit1a, limit1b)

				limit2a := NewLimit(8, time.Second)
				limit2b := NewLimit(60, time.Minute)
				limiter2 := NewLimiter(keyer2, limit2a, limit2b)

				limiters := NewLimiters(limiter1, limiter2)

				now := time.Now()

				// Should allow the first 4 requests (limited by limiter1's per-second limit)
				for i := range 4 {
					allowed := limiters.allowN("test", now, 1)
					require.True(t, allowed, "request %d should be allowed", i+1)
				}

				// The 5th request should be denied (limiter1's per-second bucket exhausted)
				allowed := limiters.allowN("test", now, 1)
				require.False(t, allowed, "5th request should be denied due to limiter1 per-second exhaustion")

				// Using a different input should work (different bucket keys)
				allowed = limiters.allowN("different", now, 1)
				require.True(t, allowed, "request with different input should be allowed")

				// Should allow 3 more requests for different input (limiter1 allows 4 per second)
				for i := range 3 {
					allowed = limiters.allowN("different", now, 1)
					require.True(t, allowed, "request %d for different input should be allowed", i+2)
				}

				// Should deny 5th request for different input too
				allowed = limiters.allowN("different", now, 1)
				require.False(t, allowed, "5th request for different input should be denied")

				// After advancing time by one second, should refill per-second buckets
				futureTime := now.Add(time.Second)

				// Should allow 4 more requests for original input
				for i := range 4 {
					allowed = limiters.allowN("test", futureTime, 1)
					require.True(t, allowed, "request %d after refill should be allowed", i+1)
				}

				// Test multiple token consumption
				evenLaterTime := futureTime.Add(time.Second)
				allowed = limiters.allowN("test", evenLaterTime, 3)
				require.True(t, allowed, "should allow consuming 3 tokens across all limits")

				// Should have 1 token remaining in limiter1's per-second bucket
				allowed = limiters.allowN("test", evenLaterTime, 1)
				require.True(t, allowed, "should allow consuming remaining 1 token")

				// Should deny further requests
				allowed = limiters.allowN("test", evenLaterTime, 1)
				require.False(t, allowed, "should deny when limiter1 per-second bucket exhausted")
			})

			t.Run("Concurrent", func(t *testing.T) {
				t.Parallel()

				limit1a := NewLimit(4, time.Second)  // 4 requests per second (most restrictive)
				limit1b := NewLimit(60, time.Minute) // 60 requests per minute
				limiter1 := NewLimiter(keyer1, limit1a, limit1b)

				limit2a := NewLimit(8, time.Second)  // 8 requests per second
				limit2b := NewLimit(40, time.Minute) // 40 requests per minute (more restrictive per-minute)
				limiter2 := NewLimiter(keyer2, limit2a, limit2b)

				limiters := NewLimiters(limiter1, limiter2)

				now := time.Now()
				const concurrency = 15

				// Run 15 goroutines concurrently, expecting only 4 to succeed (limited by limiter1's per-second)
				results := make([]bool, concurrency)

				var wg sync.WaitGroup
				for i := range concurrency {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						results[index] = limiters.allowN("test", now, 1)
					}(i)
				}
				wg.Wait()

				// Count successful requests
				successes := 0
				for _, result := range results {
					if result {
						successes++
					}
				}

				// Should allow exactly 4 requests (limited by limiter1's per-second limit)
				require.Equal(t, 4, successes, "should allow exactly 4 concurrent requests due to limiter1 per-second restriction")

				// Test concurrent access with different input (different bucket keys)
				results2 := make([]bool, concurrency)
				for i := range concurrency {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						results2[index] = limiters.allowN("different", now, 1)
					}(i)
				}
				wg.Wait()

				// Count successful requests for different input
				successCount2 := 0
				for _, result := range results2 {
					if result {
						successCount2++
					}
				}

				// Should allow exactly 4 requests for different input too
				require.Equal(t, 4, successCount2, "should allow exactly 4 concurrent requests for different input")

				// Test concurrent access after refill
				futureTime := now.Add(time.Second)
				results3 := make([]bool, concurrency)

				for i := range concurrency {
					wg.Add(1)
					go func(index int) {
						defer wg.Done()
						results3[index] = limiters.allowN("test", futureTime, 1)
					}(i)
				}
				wg.Wait()

				// Count successful requests after refill
				successCount3 := 0
				for _, result := range results3 {
					if result {
						successCount3++
					}
				}

				// Should allow exactly 4 more requests after refill
				require.Equal(t, 4, successCount3, "should allow exactly 4 concurrent requests after refill")
			})
		})
	})
}

func TestLimiters_AllowN_NoLimiters(t *testing.T) {
	t.Parallel()

	// Create Limiters with no limiters
	limiters := NewLimiters[string, string]()

	now := time.Now()

	// Should allow everything when no limiters are present
	allowed := limiters.allowN("test", now, 1)
	require.True(t, allowed, "should allow when no limiters are present")

	allowed = limiters.allowN("test", now, 100)
	require.True(t, allowed, "should allow large requests when no limiters are present")
}

func TestLimiters_AllowN_MultipleTokens(t *testing.T) {
	t.Parallel()

	// Create a simple keyer function
	keyer := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("SingleLimiter", func(t *testing.T) {
		t.Parallel()

		// Create a limiter with 10 requests per second
		limit := NewLimit(10, time.Second)
		limiter := NewLimiter(keyer, limit)

		// Create Limiters with the single limiter
		limiters := NewLimiters(limiter)

		now := time.Now()

		// Should allow consuming 5 tokens at once
		allowed := limiters.allowN("test", now, 5)
		require.True(t, allowed, "should allow consuming 5 tokens")

		// Should allow consuming 3 more tokens (8 total consumed, 2 remaining)
		allowed = limiters.allowN("test", now, 3)
		require.True(t, allowed, "should allow consuming 3 more tokens")

		// Should deny consuming 3 tokens (would exceed remaining 2)
		allowed = limiters.allowN("test", now, 3)
		require.False(t, allowed, "should deny consuming 3 tokens when only 2 remain")

		// Should allow consuming the remaining 2 tokens
		allowed = limiters.allowN("test", now, 2)
		require.True(t, allowed, "should allow consuming remaining 2 tokens")

		// Should deny any further requests
		allowed = limiters.allowN("test", now, 1)
		require.False(t, allowed, "should deny request when bucket is exhausted")
	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		t.Parallel()

		// Create two limiters with different limits
		limit1 := NewLimit(8, time.Second)  // 8 requests per second (more restrictive)
		limit2 := NewLimit(12, time.Second) // 12 requests per second
		limiter1 := NewLimiter(keyer, limit1)
		limiter2 := NewLimiter(keyer, limit2)

		// Create Limiters with both limiters
		limiters := NewLimiters(limiter1, limiter2)

		now := time.Now()

		// Should allow consuming 4 tokens at once (limited by limiter1's 8 req/sec)
		allowed := limiters.allowN("test", now, 4)
		require.True(t, allowed, "should allow consuming 4 tokens")

		// Should allow consuming 2 more tokens (6 total consumed, 2 remaining in limiter1)
		allowed = limiters.allowN("test", now, 2)
		require.True(t, allowed, "should allow consuming 2 more tokens")

		// Should deny consuming 3 tokens (would exceed limiter1's remaining 2)
		allowed = limiters.allowN("test", now, 3)
		require.False(t, allowed, "should deny consuming 3 tokens when limiter1 has only 2 remaining")

		// Should allow consuming the remaining 2 tokens from limiter1
		allowed = limiters.allowN("test", now, 2)
		require.True(t, allowed, "should allow consuming remaining 2 tokens")

		// Should deny any further requests (limiter1 exhausted)
		allowed = limiters.allowN("test", now, 1)
		require.False(t, allowed, "should deny request when limiter1 is exhausted")

		// After advancing time to refill limiter1, should allow more multiple token requests
		futureTime := now.Add(time.Second)
		allowed = limiters.allowN("test", futureTime, 6)
		require.True(t, allowed, "should allow consuming 6 tokens after refill")

		// Should have 2 tokens remaining in limiter1
		allowed = limiters.allowN("test", futureTime, 2)
		require.True(t, allowed, "should allow consuming final 2 tokens")

		// Should deny further requests
		allowed = limiters.allowN("test", futureTime, 1)
		require.False(t, allowed, "should deny when limiter1 exhausted again")
	})
}
