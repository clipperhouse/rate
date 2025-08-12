package rate

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLimiters_PeekN tests the new peekN functionality for multiple limiters
func TestLimiters_PeekN(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return input
	}

	t.Run("NoLimiters", func(t *testing.T) {
		limiters := Combine[string, string]()
		require.True(t, limiters.PeekN("test", 1), "should allow when no limiters")
		require.True(t, limiters.PeekN("test", 100), "should allow any amount when no limiters")
	})

	t.Run("SingleLimiter", func(t *testing.T) {
		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		now := time.Now()

		// Should allow peeking up to the limit
		require.True(t, limiters.peekN("test", now, 5), "should allow peeking up to limit")
		require.False(t, limiters.peekN("test", now, 6), "should not allow peeking beyond limit")

		// Consume some tokens
		require.True(t, limiters.allowN("test", now, 3))

		// Peek should now show reduced availability
		require.True(t, limiters.peekN("test", now, 2), "should allow peeking remaining tokens")
		require.False(t, limiters.peekN("test", now, 3), "should not allow peeking more than remaining")
	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		// Create two limiters with different limits
		limit1 := NewLimit(3, time.Second) // 3 per second
		limit2 := NewLimit(5, time.Minute) // 5 per minute

		limiter1 := NewLimiter(keyFunc, limit1)
		limiter2 := NewLimiter(keyFunc, limit2)
		limiters := Combine(limiter1, limiter2)

		now := time.Now()

		// Should allow peeking up to the most restrictive limit (3)
		require.True(t, limiters.peekN("test", now, 3), "should allow peeking up to most restrictive limit")
		require.False(t, limiters.peekN("test", now, 4), "should not allow peeking beyond most restrictive limit")

		// Consume tokens from both limiters
		require.True(t, limiters.allowN("test", now, 2))

		// Peek should now show reduced availability
		require.True(t, limiters.peekN("test", now, 1), "should allow peeking remaining tokens")
		require.False(t, limiters.peekN("test", now, 2), "should not allow peeking more than remaining")
	})

	t.Run("DifferentKeyers", func(t *testing.T) {
		// Create limiters with different key functions
		keyFunc1 := func(input string) string { return input + "-1" }
		keyFunc2 := func(input string) string { return input + "-2" }

		limit1 := NewLimit(3, time.Second)
		limit2 := NewLimit(5, time.Second)

		limiter1 := NewLimiter(keyFunc1, limit1)
		limiter2 := NewLimiter(keyFunc2, limit2)
		limiters := Combine(limiter1, limiter2)

		now := time.Now()

		// Should allow peeking up to the most restrictive limit (3)
		require.True(t, limiters.peekN("test", now, 3), "should allow peeking up to most restrictive limit")
		require.False(t, limiters.peekN("test", now, 4), "should not allow peeking beyond most restrictive limit")

		// Consume tokens from both limiters
		require.True(t, limiters.allowN("test", now, 2))

		// Peek should now show reduced availability
		require.True(t, limiters.peekN("test", now, 1), "should allow peeking remaining tokens")
		require.False(t, limiters.peekN("test", now, 2), "should not allow peeking more than remaining")
	})

	t.Run("PublicAPI", func(t *testing.T) {
		// Test the public API methods
		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		// Test Peek (single token)
		require.True(t, limiters.Peek("test"), "Peek should return true when tokens available")

		// Test PeekN (multiple tokens)
		require.True(t, limiters.PeekN("test", 3), "PeekN should return true for 3 tokens")
		require.False(t, limiters.PeekN("test", 6), "PeekN should return false for 6 tokens when limit is 5")

		// Peeks should not consume tokens - verify by consuming with allow
		for i := range 5 {
			allowed := limiters.allowN("test", time.Now(), 1)
			require.True(t, allowed, "allow call %d should succeed - peeks should not have consumed tokens", i)
		}
	})

	t.Run("Concurrent", func(t *testing.T) {
		t.Parallel()

		t.Run("SingleLimiter", func(t *testing.T) {
			t.Parallel()

			limit := NewLimit(8, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			limiters := Combine(limiter)
			now := time.Now()

			const concurrency = 20

			// All concurrent peeks should succeed when bucket is full
			results := make([]bool, concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index] = limiters.Peek("test") // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed since peek doesn't consume tokens
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed when bucket is full", i)
			}

			// Exhaust the bucket with allow calls
			for i := range 8 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "allow call %d should succeed", i)
			}

			// All concurrent peeks should now fail
			results2 := make([]bool, concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index] = limiters.Peek("test") // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should fail since bucket is exhausted
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d should fail when bucket is exhausted", i)
			}
		})

		t.Run("MultipleLimiters", func(t *testing.T) {
			t.Parallel()

			// Create two limiters with different limits
			limit1 := NewLimit(4, time.Second) // 4 per second (more restrictive)
			limit2 := NewLimit(8, time.Second) // 8 per second

			limiter1 := NewLimiter(keyFunc, limit1)
			limiter2 := NewLimiter(keyFunc, limit2)
			limiters := Combine(limiter1, limiter2)

			now := time.Now()
			const concurrency = 15

			// All concurrent peeks should succeed when buckets are full
			results := make([]bool, concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index] = limiters.PeekN("test", 1) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed with multiple limiters", i)
			}

			// Exhaust the more restrictive limiter (limit1)
			for i := range 4 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "allow call %d should succeed", i)
			}

			// All concurrent peeks should now fail (limited by limiter1)
			results2 := make([]bool, concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index] = limiters.PeekN("test", 1) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should fail since limiter1 is exhausted
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d should fail when most restrictive limiter is exhausted", i)
			}

			// Test concurrent peeks after refill
			futureTime := now.Add(time.Second)
			results3 := make([]bool, concurrency)

			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results3[index] = limiters.peekN("test", futureTime, 1) // Mix public/private for thorough testing
				}(i)
			}
			wg.Wait()

			// All peeks should succeed after refill
			for i, result := range results3 {
				require.True(t, result, "concurrent peek %d should succeed after refill", i)
			}
		})

		t.Run("DifferentKeyers", func(t *testing.T) {
			t.Parallel()

			// Create limiters with different key functions
			keyFunc1 := func(input string) string { return input + "-1" }
			keyFunc2 := func(input string) string { return input + "-2" }

			limit1 := NewLimit(3, time.Second)
			limit2 := NewLimit(5, time.Second)

			limiter1 := NewLimiter(keyFunc1, limit1)
			limiter2 := NewLimiter(keyFunc2, limit2)
			limiters := Combine(limiter1, limiter2)

			now := time.Now()
			const concurrency = 10

			// All concurrent peeks should succeed when buckets are full
			results := make([]bool, concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index] = limiters.peekN("test", now, 1)
				}(i)
			}
			wg.Wait()

			// All peeks should succeed
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed with different keyers", i)
			}

			// Exhaust the more restrictive limiter (limit1 = 3)
			for i := range 3 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "allow call %d should succeed", i)
			}

			// All concurrent peeks should now fail
			results2 := make([]bool, concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index] = limiters.peekN("test", now, 1)
				}(i)
			}
			wg.Wait()

			// All peeks should fail since limiter1 is exhausted
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d should fail when most restrictive limiter is exhausted", i)
			}

			// Test concurrent peeks with different input (different bucket keys)
			results3 := make([]bool, concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results3[index] = limiters.peekN("different", now, 1)
				}(i)
			}
			wg.Wait()

			// All peeks should succeed for different input
			for i, result := range results3 {
				require.True(t, result, "concurrent peek %d should succeed for different input", i)
			}
		})

		t.Run("MultipleBuckets", func(t *testing.T) {
			t.Parallel()

			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			limiters := Combine(limiter)

			now := time.Now()
			const concurrency = 10
			const buckets = 3

			// Test concurrent peeks across multiple buckets
			results := make([][]bool, buckets)
			for i := range buckets {
				results[i] = make([]bool, concurrency)
			}

			var wg sync.WaitGroup
			for bucketID := range buckets {
				for i := range concurrency {
					wg.Add(1)
					go func(bucket, index int) {
						defer wg.Done()
						input := fmt.Sprintf("bucket-%d", bucket)
						results[bucket][index] = limiters.peekN(input, now, 1)
					}(bucketID, i)
				}
			}
			wg.Wait()

			// All peeks should succeed for all buckets
			for bucketID, bucketResults := range results {
				for i, result := range bucketResults {
					require.True(t, result, "concurrent peek %d for bucket %d should succeed", i, bucketID)
				}
			}

			// Exhaust one bucket
			for i := range 5 {
				allowed := limiters.allowN("bucket-0", now, 1)
				require.True(t, allowed, "allow call %d should succeed for bucket-0", i)
			}

			// Test concurrent peeks after exhausting bucket-0
			results2 := make([][]bool, buckets)
			for i := range buckets {
				results2[i] = make([]bool, concurrency)
			}

			for bucketID := range buckets {
				for i := range concurrency {
					wg.Add(1)
					go func(bucket, index int) {
						defer wg.Done()
						input := fmt.Sprintf("bucket-%d", bucket)
						results2[bucket][index] = limiters.peekN(input, now, 1)
					}(bucketID, i)
				}
			}
			wg.Wait()

			// bucket-0 should fail, others should succeed
			for bucketID, bucketResults := range results2 {
				for i, result := range bucketResults {
					if bucketID == 0 {
						require.False(t, result, "concurrent peek %d for exhausted bucket-0 should fail", i)
					} else {
						require.True(t, result, "concurrent peek %d for bucket %d should succeed", i, bucketID)
					}
				}
			}
		})

		t.Run("PeekDoesNotMutate", func(t *testing.T) {
			t.Parallel()

			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			limiters := Combine(limiter)

			now := time.Now()
			const concurrency = 50 // High concurrency to stress test

			// Run many concurrent peeks
			results := make([]bool, concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index] = limiters.Peek("test") // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed", i)
			}

			// After all those concurrent peeks, we should still have all 5 tokens
			// Test this by consuming them with allow calls
			for i := range 5 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "allow call %d should succeed - peeks should not have consumed tokens", i)
			}

			// 6th allow should fail
			allowed := limiters.allowN("test", now, 1)
			require.False(t, allowed, "6th allow should fail - bucket should now be exhausted")
		})

		t.Run("PeekMultipleTokens", func(t *testing.T) {
			t.Parallel()

			limit := NewLimit(10, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			limiters := Combine(limiter)

			now := time.Now()
			const concurrency = 20

			// Test concurrent peeks for multiple tokens
			results := make([]bool, concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					// Each peek asks for 3 tokens
					results[index] = limiters.PeekN("test", 3) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed since bucket has 10 tokens
			for i, result := range results {
				require.True(t, result, "concurrent peek %d for 3 tokens should succeed", i)
			}

			// Consume 8 tokens, leaving only 2
			allowed := limiters.allowN("test", now, 8)
			require.True(t, allowed, "should allow consuming 8 tokens")

			// Now concurrent peeks for 3 tokens should all fail
			results2 := make([]bool, concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index] = limiters.PeekN("test", 3) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should fail since only 2 tokens remain
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d for 3 tokens should fail when only 2 remain", i)
			}

			// But peeks for 2 tokens should succeed
			results3 := make([]bool, concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results3[index] = limiters.PeekN("test", 2) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks for 2 tokens should succeed
			for i, result := range results3 {
				require.True(t, result, "concurrent peek %d for 2 tokens should succeed", i)
			}
		})
	})
}
