package rate

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiters_AllowN(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("SingleLimiter", func(t *testing.T) {
		t.Parallel()

		t.Run("SingleLimit", func(t *testing.T) {
			t.Parallel()

			t.Run("Serial", func(t *testing.T) {
				t.Parallel()

				limit := NewLimit(5, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				limiters := Combine(limiter)

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
				limiter := NewLimiter(keyFunc, limit)

				limiters := Combine(limiter)

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
				limiter := NewLimiter(keyFunc, limit1, limit2)

				limiters := Combine(limiter)

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
				limiter := NewLimiter(keyFunc, limit1, limit2)

				limiters := Combine(limiter)

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

	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		t.Parallel()
		t.Run("SameKeyer", func(t *testing.T) {
			t.Parallel()

			keyFunc := func(input string) string {
				return fmt.Sprintf("bucket-%s", input)
			}

			t.Run("SingleLimits", func(t *testing.T) {
				t.Parallel()

				t.Run("Serial", func(t *testing.T) {
					t.Parallel()

					limit1 := NewLimit(3, time.Second)
					limit2 := NewLimit(5, time.Second)
					limiter1 := NewLimiter(keyFunc, limit1)
					limiter2 := NewLimiter(keyFunc, limit2)

					// Create Limiters with both limiters
					limiters := Combine(limiter1, limiter2)

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
					limiter1 := NewLimiter(keyFunc, limit1)
					limiter2 := NewLimiter(keyFunc, limit2)

					// Create Limiters with both limiters
					limiters := Combine(limiter1, limiter2)

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
					limiter1 := NewLimiter(keyFunc, limit1a, limit1b)

					limit2a := NewLimit(10, time.Second)
					limit2b := NewLimit(50, time.Minute)
					limiter2 := NewLimiter(keyFunc, limit2a, limit2b)

					limiters := Combine(limiter1, limiter2)

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
					limiter1 := NewLimiter(keyFunc, limit1a, limit1b)

					limit2a := NewLimit(8, time.Second)  // 8 requests per second
					limit2b := NewLimit(40, time.Minute) // 40 requests per minute (more restrictive per-minute)
					limiter2 := NewLimiter(keyFunc, limit2a, limit2b)

					limiters := Combine(limiter1, limiter2)

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

			keyFunc1 := func(input string) string {
				return fmt.Sprintf("limiter1-%s", input)
			}
			keyFunc2 := func(input string) string {
				return fmt.Sprintf("limiter2-%s", input)
			}

			t.Run("SingleLimit", func(t *testing.T) {
				t.Parallel()

				t.Run("Serial", func(t *testing.T) {
					t.Parallel()

					limit1 := NewLimit(3, time.Second)
					limit2 := NewLimit(5, time.Second)
					limiter1 := NewLimiter(keyFunc1, limit1)
					limiter2 := NewLimiter(keyFunc2, limit2)

					// Create Limiters with both limiters
					limiters := Combine(limiter1, limiter2)

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
					limiter1 := NewLimiter(keyFunc1, limit1)
					limiter2 := NewLimiter(keyFunc2, limit2)

					// Create Limiters with both limiters
					limiters := Combine(limiter1, limiter2)

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
					limiter1 := NewLimiter(keyFunc1, limit1a, limit1b)

					limit2a := NewLimit(8, time.Second)
					limit2b := NewLimit(60, time.Minute)
					limiter2 := NewLimiter(keyFunc2, limit2a, limit2b)

					limiters := Combine(limiter1, limiter2)

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
					limiter1 := NewLimiter(keyFunc1, limit1a, limit1b)

					limit2a := NewLimit(8, time.Second)  // 8 requests per second
					limit2b := NewLimit(40, time.Minute) // 40 requests per minute (more restrictive per-minute)
					limiter2 := NewLimiter(keyFunc2, limit2a, limit2b)

					limiters := Combine(limiter1, limiter2)

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
	})

	t.Run("NoLimiters", func(t *testing.T) {
		t.Parallel()

		// Create Limiters with no limiters
		limiters := Combine[string, string]()

		now := time.Now()

		// Should allow everything when no limiters are present
		allowed := limiters.allowN("test", now, 1)
		require.True(t, allowed, "should allow when no limiters are present")

		allowed = limiters.allowN("test", now, 100)
		require.True(t, allowed, "should allow large requests when no limiters are present")
	})
}

func TestLimiters_AllowN_MultipleTokens(t *testing.T) {
	t.Parallel()

	// Create a simple keyFunc function
	keyFunc := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("SingleLimiter", func(t *testing.T) {
		t.Parallel()

		// Create a limiter with 10 requests per second
		limit := NewLimit(10, time.Second)
		limiter := NewLimiter(keyFunc, limit)

		// Create Limiters with the single limiter
		limiters := Combine(limiter)

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
		limiter1 := NewLimiter(keyFunc, limit1)
		limiter2 := NewLimiter(keyFunc, limit2)

		// Create Limiters with both limiters
		limiters := Combine(limiter1, limiter2)

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

func TestLimiters_AllowNWithDebug(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("NoLimiters", func(t *testing.T) {
		t.Parallel()

		limiters := Combine[string, string]()
		now := time.Now()

		allowed, debugs := limiters.allowNWithDebug("test", now, 1)
		require.True(t, allowed, "should allow when no limiters present")
		require.Empty(t, debugs, "debug slice should be empty when no limiters present")
	})

	t.Run("SingleLimiter", func(t *testing.T) {
		t.Parallel()

		t.Run("SingleLimit", func(t *testing.T) {
			t.Parallel()

			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			limiters := Combine(limiter)
			now := time.Now()

			// First allow - should succeed
			allowed, debugs := limiters.allowNWithDebug("test", now, 1)
			require.True(t, allowed, "first request should be allowed")
			require.Len(t, debugs, 1, "should have debug info for single limit")

			d := debugs[0]
			require.True(t, d.Allowed(), "debug should show allowed")
			require.Equal(t, "test", d.Input(), "input should match")
			require.Equal(t, "bucket-test", d.Key(), "key should match keyFunc output")
			require.Equal(t, limit, d.Limit(), "limit should match")
			require.Equal(t, now, d.ExecutionTime(), "execution time should match")
			require.Equal(t, int64(1), d.TokensRequested(), "tokens requested should be 1")
			require.Equal(t, int64(1), d.TokensConsumed(), "tokens consumed should be 1 when allowed")
			require.Equal(t, limit.Count()-1, d.TokensRemaining(), "tokens remaining should decrease by 1")
			require.Equal(t, time.Duration(0), d.RetryAfter(), "retry after should be 0 when allowed")

			// Consume remaining tokens
			allowed, debugs = limiters.allowNWithDebug("test", now, 4)
			require.True(t, allowed, "should allow consuming remaining 4 tokens")
			require.Len(t, debugs, 1, "should have debug info for single limit")

			d = debugs[0]
			require.True(t, d.Allowed(), "debug should show allowed")
			require.Equal(t, int64(4), d.TokensRequested(), "tokens requested should be 4")
			require.Equal(t, int64(4), d.TokensConsumed(), "tokens consumed should be 4 when allowed")
			require.Equal(t, int64(0), d.TokensRemaining(), "tokens remaining should be 0 after exhaustion")

			// Try to consume when exhausted - should fail
			allowed, debugs = limiters.allowNWithDebug("test", now, 1)
			require.False(t, allowed, "should deny when bucket exhausted")
			require.Len(t, debugs, 1, "should have debug info for single limit")

			d = debugs[0]
			require.False(t, d.Allowed(), "debug should show denied")
			require.Equal(t, int64(1), d.TokensRequested(), "tokens requested should be 1")
			require.Equal(t, int64(0), d.TokensConsumed(), "tokens consumed should be 0 when denied")
			require.Equal(t, int64(0), d.TokensRemaining(), "tokens remaining should be 0")
			expectedRetryAfter := limit.DurationPerToken()
			require.Equal(t, expectedRetryAfter, d.RetryAfter(), "retry after should be time for 1 token to refill")
		})

		t.Run("MultipleLimits", func(t *testing.T) {
			t.Parallel()

			perSecond := NewLimit(3, time.Second)
			perMinute := NewLimit(10, time.Minute)
			limiter := NewLimiter(keyFunc, perSecond, perMinute)
			limiters := Combine(limiter)
			now := time.Now()

			// First allow - should succeed
			allowed, debugs := limiters.allowNWithDebug("test", now, 1)
			require.True(t, allowed, "first request should be allowed")
			require.Len(t, debugs, 2, "should have debug info for both limits")

			// Check per-second limit debug
			d0 := debugs[0]
			require.True(t, d0.Allowed(), "per-second limit should allow")
			require.Equal(t, "test", d0.Input(), "input should match")
			require.Equal(t, "bucket-test", d0.Key(), "key should match")
			require.Equal(t, perSecond, d0.Limit(), "limit should be per-second")
			require.Equal(t, int64(1), d0.TokensConsumed(), "per-second tokens consumed should be 1")
			require.Equal(t, perSecond.Count()-1, d0.TokensRemaining(), "per-second remaining should decrease")

			// Check per-minute limit debug
			d1 := debugs[1]
			require.True(t, d1.Allowed(), "per-minute limit should allow")
			require.Equal(t, "test", d1.Input(), "input should match")
			require.Equal(t, "bucket-test", d1.Key(), "key should match")
			require.Equal(t, perMinute, d1.Limit(), "limit should be per-minute")
			require.Equal(t, int64(1), d1.TokensConsumed(), "per-minute tokens consumed should be 1")
			require.Equal(t, perMinute.Count()-1, d1.TokensRemaining(), "per-minute remaining should decrease")

			// Exhaust per-second limit
			allowed, debugs = limiters.allowNWithDebug("test", now, 2)
			require.True(t, allowed, "should allow consuming remaining 2 per-second tokens")
			require.Len(t, debugs, 2, "should have debug info for both limits")

			// Try one more - should fail because per-second is exhausted
			allowed, debugs = limiters.allowNWithDebug("test", now, 1)
			require.False(t, allowed, "should deny when per-second limit exhausted")
			require.Len(t, debugs, 2, "should have debug info for both limits")

			// Check per-second limit debug (should be denied)
			d0 = debugs[0]
			require.False(t, d0.Allowed(), "per-second limit should deny")
			require.Equal(t, int64(0), d0.TokensConsumed(), "no tokens consumed when denied")
			require.Equal(t, int64(0), d0.TokensRemaining(), "per-second should be exhausted")
			rounding := time.Nanosecond
			require.Equal(t, perSecond.DurationPerToken(), d0.RetryAfter()+rounding, "per-second retry after should be token duration")

			// Check per-minute limit debug (would allow but overall denied)
			d1 = debugs[1]
			require.True(t, d1.Allowed(), "per-minute limit would allow individually")
			require.Equal(t, int64(0), d1.TokensConsumed(), "no tokens consumed when overall denied")
			require.Equal(t, perMinute.Count()-3, d1.TokensRemaining(), "per-minute should have tokens remaining")
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "per-minute retry after should be 0")
		})
	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		t.Parallel()

		t.Run("SameKeyer", func(t *testing.T) {
			t.Parallel()

			keyFunc := func(input string) string {
				return fmt.Sprintf("bucket-%s", input)
			}

			t.Run("SingleLimits", func(t *testing.T) {
				t.Parallel()

				limit1 := NewLimit(3, time.Second)
				limit2 := NewLimit(5, time.Second)
				limiter1 := NewLimiter(keyFunc, limit1)
				limiter2 := NewLimiter(keyFunc, limit2)
				limiters := Combine(limiter1, limiter2)
				now := time.Now()

				// Should allow first 3 requests (limited by limiter1)
				allowed, debugs := limiters.allowNWithDebug("test", now, 3)
				require.True(t, allowed, "should allow 3 tokens")
				require.Len(t, debugs, 2, "should have debug info for both limiters")

				// Check limiter1 debug (more restrictive)
				d0 := debugs[0]
				require.True(t, d0.Allowed(), "limiter1 should allow 3 tokens")
				require.Equal(t, limit1, d0.Limit(), "should be limit1")
				require.Equal(t, int64(3), d0.TokensConsumed(), "should consume 3 tokens")
				require.Equal(t, int64(0), d0.TokensRemaining(), "limiter1 should be exhausted")

				// Check limiter2 debug
				d1 := debugs[1]
				require.True(t, d1.Allowed(), "limiter2 should allow 3 tokens")
				require.Equal(t, limit2, d1.Limit(), "should be limit2")
				require.Equal(t, int64(3), d1.TokensConsumed(), "should consume 3 tokens")
				require.Equal(t, limit2.Count()-3, d1.TokensRemaining(), "limiter2 should have 2 tokens remaining")

				// Try one more - should fail because limiter1 is exhausted
				allowed, debugs = limiters.allowNWithDebug("test", now, 1)
				require.False(t, allowed, "should deny when limiter1 exhausted")
				require.Len(t, debugs, 2, "should have debug info for both limiters")

				// Check limiter1 debug (exhausted)
				d0 = debugs[0]
				require.False(t, d0.Allowed(), "limiter1 should deny")
				require.Equal(t, int64(0), d0.TokensConsumed(), "no tokens consumed when denied")
				require.Equal(t, int64(0), d0.TokensRemaining(), "limiter1 should remain exhausted")

				// Check limiter2 debug (would allow but overall denied)
				d1 = debugs[1]
				require.True(t, d1.Allowed(), "limiter2 would allow individually")
				require.Equal(t, int64(0), d1.TokensConsumed(), "no tokens consumed when overall denied")
				require.Equal(t, limit2.Count()-3, d1.TokensRemaining(), "limiter2 should still have tokens")
			})

			t.Run("MultipleLimits", func(t *testing.T) {
				t.Parallel()

				// Limiter1: restrictive per-second, generous per-minute
				limit1a := NewLimit(2, time.Second)
				limit1b := NewLimit(20, time.Minute)
				limiter1 := NewLimiter(keyFunc, limit1a, limit1b)

				// Limiter2: generous per-second, restrictive per-minute
				limit2a := NewLimit(10, time.Second)
				limit2b := NewLimit(5, time.Minute)
				limiter2 := NewLimiter(keyFunc, limit2a, limit2b)

				limiters := Combine(limiter1, limiter2)
				now := time.Now()

				// Should allow first 2 requests (limited by limiter1's per-second)
				allowed, debugs := limiters.allowNWithDebug("test", now, 2)
				require.True(t, allowed, "should allow 2 tokens")
				require.Len(t, debugs, 4, "should have debug info for all 4 limits (2 limiters × 2 limits each)")

				// Check all limits allowed the request
				for i, d := range debugs {
					require.True(t, d.Allowed(), "limit %d should allow", i)
					require.Equal(t, int64(2), d.TokensConsumed(), "limit %d should consume 2 tokens", i)
				}

				// Try 2 more - should fail because limiter1's per-second is exhausted
				allowed, debugs = limiters.allowNWithDebug("test", now, 2)
				require.False(t, allowed, "should deny when limiter1 per-second exhausted")
				require.Len(t, debugs, 4, "should have debug info for all 4 limits")

				// limiter1 per-second should deny
				require.False(t, debugs[0].Allowed(), "limiter1 per-second should deny")
				require.Equal(t, int64(0), debugs[0].TokensConsumed(), "no tokens consumed")

				// limiter1 per-minute should allow (would have tokens)
				require.True(t, debugs[1].Allowed(), "limiter1 per-minute should allow individually")
				require.Equal(t, int64(0), debugs[1].TokensConsumed(), "no tokens consumed when overall denied")

				// limiter2 per-second should allow
				require.True(t, debugs[2].Allowed(), "limiter2 per-second should allow individually")
				require.Equal(t, int64(0), debugs[2].TokensConsumed(), "no tokens consumed when overall denied")

				// limiter2 per-minute should allow
				require.True(t, debugs[3].Allowed(), "limiter2 per-minute should allow individually")
				require.Equal(t, int64(0), debugs[3].TokensConsumed(), "no tokens consumed when overall denied")
			})
		})

		t.Run("DifferentKeyers", func(t *testing.T) {
			t.Parallel()

			keyFunc1 := func(input string) string {
				return fmt.Sprintf("bucket1-%s", input)
			}
			keyFunc2 := func(input string) string {
				return fmt.Sprintf("bucket2-%s", input)
			}

			limit1 := NewLimit(3, time.Second)
			limit2 := NewLimit(5, time.Second)
			limiter1 := NewLimiter(keyFunc1, limit1)
			limiter2 := NewLimiter(keyFunc2, limit2)
			limiters := Combine(limiter1, limiter2)
			now := time.Now()

			// Should allow first 3 requests (limited by limiter1)
			allowed, debugs := limiters.allowNWithDebug("test", now, 3)
			require.True(t, allowed, "should allow 3 tokens with different keyers")
			require.Len(t, debugs, 2, "should have debug info for both limiters")

			// Check limiter1 debug (different key)
			d0 := debugs[0]
			require.True(t, d0.Allowed(), "limiter1 should allow")
			require.Equal(t, "test", d0.Input(), "input should match")
			require.Equal(t, "bucket1-test", d0.Key(), "key should use keyFunc1")
			require.Equal(t, limit1, d0.Limit(), "should be limit1")

			// Check limiter2 debug (different key)
			d1 := debugs[1]
			require.True(t, d1.Allowed(), "limiter2 should allow")
			require.Equal(t, "test", d1.Input(), "input should match")
			require.Equal(t, "bucket2-test", d1.Key(), "key should use keyFunc2")
			require.Equal(t, limit2, d1.Limit(), "should be limit2")
		})
	})

	t.Run("Concurrent", func(t *testing.T) {
		t.Parallel()

		limit1 := NewLimit(5, time.Second)
		limit2 := NewLimit(8, time.Second)
		limiter1 := NewLimiter(keyFunc, limit1)
		limiter2 := NewLimiter(keyFunc, limit2)
		limiters := Combine(limiter1, limiter2)
		now := time.Now()

		const concurrency = 10

		// Run concurrent requests, expecting only 5 to succeed (limited by limiter1)
		results := make([]bool, concurrency)
		debugResults := make([][]Debug[string, string], concurrency)

		var wg sync.WaitGroup
		for i := range concurrency {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				allowed, debugs := limiters.allowNWithDebug("test", now, 1)
				results[i] = allowed
				debugResults[i] = debugs
			}(i)
		}
		wg.Wait()

		// Count successes
		successes := 0
		for _, result := range results {
			if result {
				successes++
			}
		}

		// Should allow exactly 5 requests (limited by the more restrictive limiter1)
		require.Equal(t, 5, successes, "should allow exactly 5 concurrent requests")

		// Verify debug structure for all results
		for i, debugs := range debugResults {
			require.Len(t, debugs, 2, "result %d should have debug info for both limiters", i)
			if results[i] {
				// Successful requests should have tokens consumed
				require.True(t, debugs[0].Allowed(), "successful request %d limiter1 should show allowed", i)
				require.True(t, debugs[1].Allowed(), "successful request %d limiter2 should show allowed", i)
				require.Equal(t, int64(1), debugs[0].TokensConsumed(), "successful request %d should consume from limiter1", i)
				require.Equal(t, int64(1), debugs[1].TokensConsumed(), "successful request %d should consume from limiter2", i)
			} else {
				// Failed requests should have no tokens consumed
				require.Equal(t, int64(0), debugs[0].TokensConsumed(), "failed request %d should not consume from limiter1", i)
				require.Equal(t, int64(0), debugs[1].TokensConsumed(), "failed request %d should not consume from limiter2", i)
			}
		}
	})
}

func TestLimiters_AllowNWithDebug_EdgeCases(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("MixedLimitersWithZeroLimits", func(t *testing.T) {
		t.Parallel()

		// Create limiters where some have limits and others don't
		limit1 := NewLimit(3, time.Second)
		limiter1 := NewLimiter(keyFunc, limit1)

		// Create a limiter with no limits (using NewLimiterFunc with empty function list)
		limiter2 := NewLimiterFunc(keyFunc)

		limiters := Combine(limiter1, limiter2)
		now := time.Now()

		// Should be limited by limiter1 since limiter2 has no limits
		allowed, debugs := limiters.allowNWithDebug("test", now, 3)
		require.True(t, allowed, "should allow 3 tokens")
		require.Len(t, debugs, 1, "should only have debug info for limiter with actual limits")

		// Check that only limiter1 contributes to debug output
		d := debugs[0]
		require.Equal(t, limit1, d.Limit(), "should be limiter1's limit")
		require.Equal(t, int64(3), d.TokensConsumed(), "should consume 3 tokens")
		require.Equal(t, int64(0), d.TokensRemaining(), "limiter1 should be exhausted")

		// Try one more - should fail because limiter1 is exhausted
		allowed, debugs = limiters.allowNWithDebug("test", now, 1)
		require.False(t, allowed, "should deny when limiter1 exhausted")
		require.Len(t, debugs, 1, "should still only have debug for limiter with limits")
	})

	t.Run("ZeroTokenRequest", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)
		now := time.Now()

		// Test requesting 0 tokens
		allowed, debugs := limiters.allowNWithDebug("test", now, 0)
		require.True(t, allowed, "should allow 0 token request")
		require.Len(t, debugs, 1, "should have debug info")

		d := debugs[0]
		require.True(t, d.Allowed(), "should show allowed for 0 tokens")
		require.Equal(t, int64(0), d.TokensRequested(), "should request 0 tokens")
		require.Equal(t, int64(0), d.TokensConsumed(), "should consume 0 tokens")
		require.Equal(t, limit.Count(), d.TokensRemaining(), "remaining should be unchanged")
		require.Equal(t, time.Duration(0), d.RetryAfter(), "retry after should be 0")
	})

	t.Run("LargeTokenRequest", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)
		now := time.Now()

		// Test requesting more tokens than available
		allowed, debugs := limiters.allowNWithDebug("test", now, 10)
		require.False(t, allowed, "should deny large token request")
		require.Len(t, debugs, 1, "should have debug info")

		d := debugs[0]
		require.False(t, d.Allowed(), "should show denied for large request")
		require.Equal(t, int64(10), d.TokensRequested(), "should request 10 tokens")
		require.Equal(t, int64(0), d.TokensConsumed(), "should consume 0 tokens when denied")
		require.Equal(t, limit.Count(), d.TokensRemaining(), "remaining should be unchanged")

		// RetryAfter should be time needed to accumulate 10 tokens
		expectedRetryAfter := limit.DurationPerToken() * time.Duration(10-limit.Count())
		require.Equal(t, expectedRetryAfter, d.RetryAfter(), "retry after should account for needed tokens")
	})

	t.Run("LimitFuncReturningZeroCount", func(t *testing.T) {
		t.Parallel()

		// Create a limiter that conditionally returns no limits
		conditionalLimitFunc := func(input string) Limit {
			if input == "blocked" {
				// Return a very restrictive limit instead of zero count
				return NewLimit(1, time.Hour) // Only 1 token per hour
			}
			return NewLimit(5, time.Second) // Normal limit for other inputs
		}
		limiter := NewLimiterFunc(keyFunc, conditionalLimitFunc)
		limiters := Combine(limiter)
		now := time.Now()

		// Test blocked input - should allow only 1 token
		allowed, debugs := limiters.allowNWithDebug("blocked", now, 1)
		require.True(t, allowed, "should allow 1 token for blocked input")
		require.Len(t, debugs, 1, "should have debug info")

		d := debugs[0]
		require.True(t, d.Allowed(), "should show allowed for first token")
		require.Equal(t, int64(1), d.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(1), d.TokensConsumed(), "should consume 1 token")
		require.Equal(t, int64(0), d.TokensRemaining(), "should have 0 tokens remaining")

		// Second request should be denied
		allowed, debugs = limiters.allowNWithDebug("blocked", now, 1)
		require.False(t, allowed, "should deny second token for blocked input")
		require.Len(t, debugs, 1, "should have debug info")

		d = debugs[0]
		require.False(t, d.Allowed(), "should show denied")
		require.Equal(t, int64(0), d.TokensConsumed(), "should consume 0 tokens when denied")

		// Test normal input - should work fine
		allowed, debugs = limiters.allowNWithDebug("normal", now, 3)
		require.True(t, allowed, "should allow 3 tokens for normal input")
		require.Len(t, debugs, 1, "should have debug info")

		d = debugs[0]
		require.Equal(t, int64(5), d.Limit().Count(), "should use normal limit")
		require.Equal(t, int64(3), d.TokensConsumed(), "should consume 3 tokens")
		require.Equal(t, int64(2), d.TokensRemaining(), "should have 2 tokens remaining")
	})

	t.Run("EmptyKeyFromKeyFunc", func(t *testing.T) {
		t.Parallel()

		emptyKeyFunc := func(input string) string {
			return "" // Empty key
		}
		limit := NewLimit(3, time.Second)
		limiter := NewLimiter(emptyKeyFunc, limit)
		limiters := Combine(limiter)
		now := time.Now()

		// Should work fine with empty key
		allowed, debugs := limiters.allowNWithDebug("test", now, 1)
		require.True(t, allowed, "should allow with empty key")
		require.Len(t, debugs, 1, "should have debug info")

		d := debugs[0]
		require.True(t, d.Allowed(), "should show allowed")
		require.Equal(t, "test", d.Input(), "input should match")
		require.Equal(t, "", d.Key(), "key should be empty string")
		require.Equal(t, int64(1), d.TokensConsumed(), "should consume 1 token")

		// Second request should use same bucket (empty key)
		allowed, debugs = limiters.allowNWithDebug("different-input", now, 1)
		require.True(t, allowed, "should allow second request with different input")
		require.Len(t, debugs, 1, "should have debug info")

		d = debugs[0]
		require.Equal(t, "different-input", d.Input(), "input should match new input")
		require.Equal(t, "", d.Key(), "key should still be empty")
		require.Equal(t, limit.Count()-2, d.TokensRemaining(), "should share bucket with previous request")
	})

	t.Run("LimitFuncWithVariableLimits", func(t *testing.T) {
		t.Parallel()

		// Create limiter with variable limits based on input
		variableLimitFunc := func(input string) Limit {
			if input == "premium" {
				return NewLimit(10, time.Second) // Premium gets higher limit
			}
			return NewLimit(2, time.Second) // Basic gets lower limit
		}
		limiter := NewLimiterFunc(keyFunc, variableLimitFunc)
		limiters := Combine(limiter)
		now := time.Now()

		// Test premium input
		allowed, debugs := limiters.allowNWithDebug("premium", now, 5)
		require.True(t, allowed, "should allow 5 tokens for premium")
		require.Len(t, debugs, 1, "should have debug info")

		d := debugs[0]
		require.Equal(t, int64(10), d.Limit().Count(), "should use premium limit")
		require.Equal(t, int64(5), d.TokensConsumed(), "should consume 5 tokens")
		require.Equal(t, int64(5), d.TokensRemaining(), "should have 5 tokens remaining")

		// Test basic input with same token request
		allowed, debugs = limiters.allowNWithDebug("basic", now, 5)
		require.False(t, allowed, "should deny 5 tokens for basic")
		require.Len(t, debugs, 1, "should have debug info")

		d = debugs[0]
		require.Equal(t, int64(2), d.Limit().Count(), "should use basic limit")
		require.Equal(t, int64(0), d.TokensConsumed(), "should consume 0 tokens when denied")
		require.Equal(t, int64(2), d.TokensRemaining(), "should have 2 tokens available")
	})

	t.Run("StackOptimizationThresholds", func(t *testing.T) {
		t.Parallel()

		// Test with exactly the stack optimization threshold limits (6 different limits)
		limits := []Limit{
			NewLimit(1, time.Second),
			NewLimit(2, time.Second),
			NewLimit(3, time.Second),
			NewLimit(4, time.Second),
			NewLimit(5, time.Second),
			NewLimit(6, time.Second),
		}
		limiter := NewLimiter(keyFunc, limits...)
		limiters := Combine(limiter)
		now := time.Now()

		allowed, debugs := limiters.allowNWithDebug("test", now, 1)
		require.True(t, allowed, "should allow with exactly 6 limits")
		require.Len(t, debugs, 6, "should have debug info for all 6 limits")

		// Verify all limits show correct information
		for i, d := range debugs {
			require.True(t, d.Allowed(), "limit %d should allow", i)
			require.Equal(t, limits[i], d.Limit(), "limit %d should match", i)
			require.Equal(t, int64(1), d.TokensConsumed(), "limit %d should consume 1 token", i)
		}

		// Test with more than stack threshold (7 different limits)
		limits = append(limits, NewLimit(7, time.Second))
		limiter = NewLimiter(keyFunc, limits...)
		limiters = Combine(limiter)

		allowed, debugs = limiters.allowNWithDebug("test2", now, 1)
		require.True(t, allowed, "should allow with 7 limits (above stack threshold)")
		require.Len(t, debugs, 7, "should have debug info for all 7 limits")
	})

	t.Run("RefillTimingEdgeCases", func(t *testing.T) {
		t.Parallel()

		fastLimit := NewLimit(2, 100*time.Millisecond) // Very fast refill
		slowLimit := NewLimit(2, 10*time.Second)       // Very slow refill
		limiter := NewLimiter(keyFunc, fastLimit, slowLimit)
		limiters := Combine(limiter)

		baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

		// Exhaust both limits
		allowed, debugs := limiters.allowNWithDebug("test", baseTime, 2)
		require.True(t, allowed, "should allow exhausting both limits")
		require.Len(t, debugs, 2, "should have debug for both limits")

		// Verify both are exhausted
		for i, d := range debugs {
			require.Equal(t, int64(0), d.TokensRemaining(), "limit %d should be exhausted", i)
		}

		// Try request when both exhausted
		allowed, debugs = limiters.allowNWithDebug("test", baseTime, 1)
		require.False(t, allowed, "should deny when both exhausted")

		// Fast limit should have shorter retry after, slow limit should have longer
		fastDebug := debugs[0]
		slowDebug := debugs[1]
		require.Less(t, fastDebug.RetryAfter(), slowDebug.RetryAfter(),
			"fast limit should have shorter retry time than slow limit")

		// Move time forward by fast refill amount
		futureTime := baseTime.Add(fastLimit.DurationPerToken())
		allowed, debugs = limiters.allowNWithDebug("test", futureTime, 1)
		require.False(t, allowed, "should still deny due to slow limit")

		// Fast limit should now allow, slow limit should still deny
		fastDebug = debugs[0]
		slowDebug = debugs[1]
		require.True(t, fastDebug.Allowed(), "fast limit should allow after refill")
		require.False(t, slowDebug.Allowed(), "slow limit should still deny")
		require.Equal(t, int64(0), fastDebug.TokensConsumed(), "no tokens consumed when overall denied")
		require.Equal(t, int64(0), slowDebug.TokensConsumed(), "no tokens consumed when overall denied")
	})
}

func TestLimiters_AllowNWithDetails(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("NoLimiters", func(t *testing.T) {
		t.Parallel()

		limiters := Combine[string, string]()
		now := time.Now()

		allowed, d := limiters.allowNWithDetails("test", now, 1)
		require.True(t, allowed, "should allow when no limiters present")
		require.True(t, d.Allowed(), "details.Allowed should be true when no limiters present")
		require.Equal(t, int64(1), d.TokensRequested(), "tokens requested should match input")
		require.Equal(t, int64(0), d.TokensConsumed(), "no tokens consumed when unconstrained")
		require.Equal(t, int64(0), d.TokensRemaining(), "remaining tokens reported as 0 when unconstrained")
		require.Equal(t, time.Duration(0), d.RetryAfter(), "retryAfter should be 0 when allowed and unconstrained")
	})

	t.Run("SingleLimiter", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)
		now := time.Now()

		allowed, d := limiters.allowNWithDetails("test", now, 1)
		require.True(t, allowed)
		require.True(t, d.Allowed())
		require.Equal(t, int64(1), d.TokensRequested())
		require.Equal(t, int64(1), d.TokensConsumed())
		require.Equal(t, limit.Count()-1, d.TokensRemaining(), "remaining should decrement by 1")
		require.Equal(t, time.Duration(0), d.RetryAfter())

		t.Run("Identical", func(t *testing.T) {
			t.Parallel()

			now := time.Now()

			limiter := NewLimiter(keyFunc, limit)
			limiters := Combine(limiter)

			identical := NewLimiter(keyFunc, limit)

			// Test that Limiters with single Limiter behaves identically to direct Limiter usage

			// First request - both should allow
			allowed1, details1 := limiters.allowNWithDetails("test", now, 1)
			allowedDirect1, detailsDirect1 := identical.allowNWithDetails("test", now, 1)

			require.Equal(t, allowedDirect1, allowed1, "allow result should be identical")
			require.Equal(t, detailsDirect1.Allowed(), details1.Allowed(), "details.Allowed should be identical")
			require.Equal(t, detailsDirect1.TokensRequested(), details1.TokensRequested(), "tokensRequested should be identical")
			require.Equal(t, detailsDirect1.TokensConsumed(), details1.TokensConsumed(), "tokensConsumed should be identical")
			require.Equal(t, detailsDirect1.TokensRemaining(), details1.TokensRemaining(), "tokensRemaining should be identical")
			require.Equal(t, detailsDirect1.RetryAfter(), details1.RetryAfter(), "retryAfter should be identical")

			// Second request - both should allow
			allowed2, details2 := limiters.allowNWithDetails("test", now, 2)
			allowedDirect2, detailsDirect2 := identical.allowNWithDetails("test", now, 2)

			require.Equal(t, allowedDirect2, allowed2, "allow result should be identical for multi-token request")
			require.Equal(t, detailsDirect2.Allowed(), details2.Allowed(), "details.Allowed should be identical")
			require.Equal(t, detailsDirect2.TokensConsumed(), details2.TokensConsumed(), "tokensConsumed should be identical")
			require.Equal(t, detailsDirect2.TokensRemaining(), details2.TokensRemaining(), "tokensRemaining should be identical")
			require.Equal(t, detailsDirect2.RetryAfter(), details2.RetryAfter(), "retryAfter should be identical")

			// Third request - both should deny (bucket exhausted)
			allowed3, details3 := limiters.allowNWithDetails("test", now, 1)
			allowedDirect3, detailsDirect3 := identical.allowNWithDetails("test", now, 1)

			require.Equal(t, allowedDirect3, allowed3, "deny result should be identical")
			require.Equal(t, detailsDirect3.Allowed(), details3.Allowed(), "details.Allowed should be identical when denied")
			require.Equal(t, detailsDirect3.TokensConsumed(), details3.TokensConsumed(), "tokensConsumed should be identical when denied")
			require.Equal(t, detailsDirect3.TokensRemaining(), details3.TokensRemaining(), "tokensRemaining should be identical when denied")
			require.Equal(t, detailsDirect3.RetryAfter(), details3.RetryAfter(), "retryAfter should be identical when denied")
		})
	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		t.Parallel()

		now := time.Now()

		// Limiter1: more restrictive per-second, generous per-minute
		perSecond1 := NewLimit(2, time.Second)  // 500ms/token
		perMinute1 := NewLimit(20, time.Minute) // 3s/token
		limiter1 := NewLimiter(keyFunc, perSecond1, perMinute1)

		// Limiter2: less restrictive per-second, more restrictive per-minute
		perSecond2 := NewLimit(3, time.Second)  // ~333ms/token
		perMinute2 := NewLimit(10, time.Minute) // 6s/token
		limiter2 := NewLimiter(keyFunc, perSecond2, perMinute2)

		limiters := Combine(limiter1, limiter2)

		// First allow
		allowed1, d1 := limiters.allowNWithDetails("acct", now, 1)
		require.True(t, allowed1)
		require.True(t, d1.Allowed())

		// Remaining should be minimum across all 4 buckets:
		// perSecond1: 1 left, perMinute1: 19, perSecond2: 2, perMinute2: 9 => min=1
		require.Equal(t, int64(1), d1.TokensRemaining(), "remaining should be min across buckets")
		require.Equal(t, int64(1), d1.TokensConsumed())
		require.Equal(t, time.Duration(0), d1.RetryAfter(), "retryAfter should be 0 when allowed")

		// Second allow (consumes perSecond1 second token)
		allowed2, d2 := limiters.allowNWithDetails("acct", now, 1)
		require.True(t, allowed2)

		// Remaining now min across: perSecond1:0, perMinute1:18, perSecond2:1, perMinute2:8 => 0
		require.Equal(t, int64(0), d2.TokensRemaining(), "remaining should drop to 0 (most restrictive exhausted)")
		require.Equal(t, int64(1), d2.TokensConsumed())
		require.Equal(t, time.Duration(0), d2.RetryAfter())

		// Third allow denied (per-second bucket in limiter1 exhausted)
		allowed3, d3 := limiters.allowNWithDetails("acct", now, 1)
		require.False(t, allowed3, "should be denied due to exhausted fastest bucket")
		require.False(t, d3.Allowed())
		require.Equal(t, int64(0), d3.TokensConsumed(), "no tokens consumed on denial")

		// RetryAfter should be max(next token wait across buckets). Per-second exhausted needs 500ms.
		// Other buckets still have tokens so their retryAfter <=0. Expect 500ms.
		perSecWait := perSecond1.DurationPerToken()
		require.Equal(t, perSecWait, d3.RetryAfter(), "retryAfter should be exactly one token duration for exhausted per-second bucket")
	})
}

func TestLimiters_PublicMethods(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("Allow", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(3, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		// Should allow first 3 requests
		for i := range 3 {
			allowed := limiters.Allow("test")
			require.True(t, allowed, "request %d should be allowed", i+1)
		}

		// 4th request should be denied
		allowed := limiters.Allow("test")
		require.False(t, allowed, "4th request should be denied")

		// Wait for refill and try again
		time.Sleep(limit.DurationPerToken())
		allowed = limiters.Allow("test")
		require.True(t, allowed, "request after refill should be allowed")
	})

	t.Run("AllowN", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(10, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		// Should allow consuming 5 tokens at once
		allowed := limiters.AllowN("test", 5)
		require.True(t, allowed, "should allow consuming 5 tokens")

		// Should allow consuming 3 more tokens (8 total consumed, 2 remaining)
		allowed = limiters.AllowN("test", 3)
		require.True(t, allowed, "should allow consuming 3 more tokens")

		// Should deny consuming 3 tokens (would exceed remaining 2)
		allowed = limiters.AllowN("test", 3)
		require.False(t, allowed, "should deny consuming 3 tokens when only 2 remain")

		// Should allow consuming the remaining 2 tokens
		allowed = limiters.AllowN("test", 2)
		require.True(t, allowed, "should allow consuming remaining 2 tokens")

		// Should deny any further requests
		allowed = limiters.AllowN("test", 1)
		require.False(t, allowed, "should deny request when bucket is exhausted")
	})

	t.Run("AllowWithDetails", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		// First request - should succeed
		allowed, details := limiters.AllowWithDetails("test")
		require.True(t, allowed, "first request should be allowed")
		require.True(t, details.Allowed(), "details should show allowed")
		require.Equal(t, int64(1), details.TokensRequested(), "should request 1 token")
		require.Equal(t, int64(1), details.TokensConsumed(), "should consume 1 token")
		require.Equal(t, limit.Count()-1, details.TokensRemaining(), "remaining should decrease by 1")
		require.Equal(t, time.Duration(0), details.RetryAfter(), "retry after should be 0 when allowed")

		// Consume remaining tokens
		allowed, details = limiters.AllowWithDetails("test")
		require.True(t, allowed, "should allow consuming remaining tokens")
		require.Equal(t, int64(1), details.TokensConsumed(), "should consume 1 token")
		require.Equal(t, limit.Count()-2, details.TokensRemaining(), "remaining should decrease further")

		// Exhaust bucket
		for i := range 3 {
			allowed, _ = limiters.AllowWithDetails("test")
			require.True(t, allowed, "request %d should be allowed", i+1)
		}

		// Try one more - should fail
		allowed, details = limiters.AllowWithDetails("test")
		require.False(t, allowed, "should deny when bucket exhausted")
		require.False(t, details.Allowed(), "details should show denied")
		require.Equal(t, int64(0), details.TokensConsumed(), "no tokens consumed when denied")
		require.Equal(t, int64(0), details.TokensRemaining(), "remaining should be 0")
		require.Greater(t, details.RetryAfter(), time.Duration(0), "retry after should be positive when denied")
	})

	t.Run("AllowNWithDetails", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(8, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		// Should allow consuming 3 tokens
		allowed, details := limiters.AllowNWithDetails("test", 3)
		require.True(t, allowed, "should allow consuming 3 tokens")
		require.True(t, details.Allowed(), "details should show allowed")
		require.Equal(t, int64(3), details.TokensRequested(), "should request 3 tokens")
		require.Equal(t, int64(3), details.TokensConsumed(), "should consume 3 tokens")
		require.Equal(t, limit.Count()-3, details.TokensRemaining(), "remaining should decrease by 3")

		// Should allow consuming 4 more tokens (7 total consumed, 1 remaining)
		allowed, details = limiters.AllowNWithDetails("test", 4)
		require.True(t, allowed, "should allow consuming 4 more tokens")
		require.Equal(t, int64(4), details.TokensConsumed(), "should consume 4 tokens")
		require.Equal(t, int64(1), details.TokensRemaining(), "should have 1 token remaining")

		// Should deny consuming 2 tokens (would exceed remaining 1)
		allowed, details = limiters.AllowNWithDetails("test", 2)
		require.False(t, allowed, "should deny consuming 2 tokens when only 1 remains")
		require.False(t, details.Allowed(), "details should show denied")
		require.Equal(t, int64(0), details.TokensConsumed(), "no tokens consumed when denied")
		require.Equal(t, int64(1), details.TokensRemaining(), "remaining should be unchanged")

		// Should allow consuming the last token
		allowed, details = limiters.AllowNWithDetails("test", 1)
		require.True(t, allowed, "should allow consuming last token")
		require.Equal(t, int64(0), details.TokensRemaining(), "should have 0 tokens remaining")
	})

	t.Run("AllowWithDebug", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(4, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		// First request - should succeed
		allowed, debugs := limiters.AllowWithDebug("test")
		require.True(t, allowed, "first request should be allowed")
		require.Len(t, debugs, 1, "should have debug info for single limit")

		d := debugs[0]
		require.True(t, d.Allowed(), "debug should show allowed")
		require.Equal(t, "test", d.Input(), "input should match")
		require.Equal(t, "bucket-test", d.Key(), "key should match keyFunc output")
		require.Equal(t, limit, d.Limit(), "limit should match")
		require.Equal(t, int64(1), d.TokensRequested(), "tokens requested should be 1")
		require.Equal(t, int64(1), d.TokensConsumed(), "tokens consumed should be 1 when allowed")
		require.Equal(t, limit.Count()-1, d.TokensRemaining(), "tokens remaining should decrease by 1")
		require.Equal(t, time.Duration(0), d.RetryAfter(), "retry after should be 0 when allowed")

		// Consume remaining tokens
		allowed, debugs = limiters.AllowWithDebug("test")
		require.True(t, allowed, "should allow consuming remaining tokens")
		require.Len(t, debugs, 1, "should have debug info")

		d = debugs[0]
		require.True(t, d.Allowed(), "debug should show allowed")
		require.Equal(t, int64(1), d.TokensConsumed(), "should consume 1 token")
		require.Equal(t, limit.Count()-2, d.TokensRemaining(), "remaining should decrease further")
		require.Equal(t, time.Duration(0), d.RetryAfter(), "retry after should be 0 when allowed")

		// Exhaust bucket
		for i := range 2 {
			allowed, _ = limiters.AllowWithDebug("test")
			require.True(t, allowed, "request %d should be allowed", i+1)
		}

		// Try one more - should fail
		allowed, debugs = limiters.AllowWithDebug("test")
		require.False(t, allowed, "should deny when bucket exhausted")
		require.Len(t, debugs, 1, "should have debug info")

		d = debugs[0]
		require.False(t, d.Allowed(), "debug should show denied")
		require.Equal(t, int64(0), d.TokensConsumed(), "no tokens consumed when denied")
		require.Equal(t, int64(0), d.TokensRemaining(), "remaining should be 0")
		// bit of a fudge factor here since we are depending on a real system clock
		require.Greater(t, d.RetryAfter(), time.Duration(248*time.Millisecond), "retry after should be greater than 248ms when denied")
		require.Less(t, d.RetryAfter(), time.Duration(252*time.Millisecond), "retry after should be less than 252ms when denied")
	})

	t.Run("AllowNWithDebug", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(6, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		// Should allow consuming 2 tokens
		allowed, debugs := limiters.AllowNWithDebug("test", 2)
		require.True(t, allowed, "should allow consuming 2 tokens")
		require.Len(t, debugs, 1, "should have debug info")

		d := debugs[0]
		require.True(t, d.Allowed(), "debug should show allowed")
		require.Equal(t, int64(2), d.TokensRequested(), "should request 2 tokens")
		require.Equal(t, int64(2), d.TokensConsumed(), "should consume 2 tokens")
		require.Equal(t, limit.Count()-2, d.TokensRemaining(), "remaining should decrease by 2")

		// Should allow consuming 3 more tokens (5 total consumed, 1 remaining)
		allowed, debugs = limiters.AllowNWithDebug("test", 3)
		require.True(t, allowed, "should allow consuming 3 more tokens")
		require.Len(t, debugs, 1, "should have debug info")

		d = debugs[0]
		require.True(t, d.Allowed(), "debug should show allowed")
		require.Equal(t, int64(3), d.TokensConsumed(), "should consume 3 tokens")
		require.Equal(t, int64(1), d.TokensRemaining(), "should have 1 token remaining")

		// Should deny consuming 2 tokens (would exceed remaining 1)
		allowed, debugs = limiters.AllowNWithDebug("test", 2)
		require.False(t, allowed, "should deny consuming 2 tokens when only 1 remains")
		require.Len(t, debugs, 1, "should have debug info")

		d = debugs[0]
		require.False(t, d.Allowed(), "debug should show denied")
		require.Equal(t, int64(0), d.TokensConsumed(), "no tokens consumed when denied")
		require.Equal(t, int64(1), d.TokensRemaining(), "remaining should be unchanged")

		// Should allow consuming the last token
		allowed, debugs = limiters.AllowNWithDebug("test", 1)
		require.True(t, allowed, "should allow consuming last token")
		require.Len(t, debugs, 1, "should have debug info")

		d = debugs[0]
		require.True(t, d.Allowed(), "debug should show allowed")
		require.Equal(t, int64(0), d.TokensRemaining(), "should have 0 tokens remaining")
	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		t.Parallel()

		limit1 := NewLimit(3, time.Second)
		limit2 := NewLimit(5, time.Second)
		limiter1 := NewLimiter(keyFunc, limit1)
		limiter2 := NewLimiter(keyFunc, limit2)
		limiters := Combine(limiter1, limiter2)

		// Should allow first 3 requests (limited by limiter1)
		for i := range 3 {
			allowed := limiters.Allow("test")
			require.True(t, allowed, "request %d should be allowed", i+1)
		}

		// 4th request should be denied (limiter1 exhausted)
		allowed := limiters.Allow("test")
		require.False(t, allowed, "4th request should be denied")

		// Test with AllowN
		allowed = limiters.AllowN("test", 2)
		require.False(t, allowed, "should deny 2 tokens when limiter1 exhausted")

		// Test with AllowWithDetails
		allowed, details := limiters.AllowWithDetails("test")
		require.False(t, allowed, "should deny when limiter1 exhausted")
		require.False(t, details.Allowed(), "details should show denied")
		require.Equal(t, int64(0), details.TokensConsumed(), "no tokens consumed when denied")

		// Test with AllowWithDebug
		allowed, debugs := limiters.AllowWithDebug("test")
		require.False(t, allowed, "should deny when limiter1 exhausted")
		require.Len(t, debugs, 2, "should have debug info for both limiters")
		require.False(t, debugs[0].Allowed(), "limiter1 should show denied")
		require.True(t, debugs[1].Allowed(), "limiter2 would allow individually")
	})

	t.Run("NoLimiters", func(t *testing.T) {
		t.Parallel()

		limiters := Combine[string, string]()

		// All methods should allow when no limiters present
		allowed := limiters.Allow("test")
		require.True(t, allowed, "Allow should allow when no limiters present")

		allowed = limiters.AllowN("test", 100)
		require.True(t, allowed, "AllowN should allow when no limiters present")

		allowed, details := limiters.AllowWithDetails("test")
		require.True(t, allowed, "AllowWithDetails should allow when no limiters present")
		require.True(t, details.Allowed(), "details should show allowed")
		require.Equal(t, int64(1), details.TokensRequested(), "tokens requested should match")
		require.Equal(t, int64(0), details.TokensConsumed(), "no tokens consumed when unconstrained")

		allowed, debugs := limiters.AllowWithDebug("test")
		require.True(t, allowed, "AllowWithDebug should allow when no limiters present")
		require.Empty(t, debugs, "debug slice should be empty when no limiters present")
	})
}
