package rate

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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

		now := bnow()

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

		now := bnow()

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

		now := bnow()

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
			allowed := limiters.allowN("test", bnow(), 1)
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
			now := bnow()

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

			now := bnow()
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

			now := bnow()
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

			now := bnow()
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

			now := bnow()
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

			now := bnow()
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

func TestLimiters_PeekNWithDetails(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return input
	}

	t.Run("NoLimiters", func(t *testing.T) {
		limiters := Combine[string, string]()
		allowed, details := limiters.PeekNWithDetails("test", 1)
		require.True(t, allowed, "should allow when no limiters")
		require.Equal(t, int64(1), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed(), "peek never consumes tokens")
		require.Equal(t, int64(0), details.TokensRemaining(), "should show 0 remaining when no limits")
		require.Equal(t, time.Duration(0), details.RetryAfter(), "should show 0 retry after when no limits")

		allowed, details = limiters.PeekNWithDetails("test", 100)
		require.True(t, allowed, "should allow any amount when no limiters")
		require.Equal(t, int64(100), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed())
		require.Equal(t, int64(0), details.TokensRemaining())
		require.Equal(t, time.Duration(0), details.RetryAfter())
	})

	t.Run("SingleLimiter", func(t *testing.T) {
		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		now := bnow()

		// Should allow peeking up to the limit
		allowed, details := limiters.peekNWithDetails("test", now, 5)
		require.True(t, allowed, "should allow peeking up to limit")
		require.Equal(t, int64(5), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed(), "peek never consumes tokens")
		require.Equal(t, int64(5), details.TokensRemaining(), "should show all tokens remaining")
		require.Equal(t, time.Duration(0), details.RetryAfter(), "should show 0 retry after when available")

		allowed, details = limiters.peekNWithDetails("test", now, 6)
		require.False(t, allowed, "should not allow peeking beyond limit")
		require.Equal(t, int64(6), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed(), "peek never consumes tokens")
		require.Equal(t, int64(5), details.TokensRemaining(), "should show current remaining tokens")
		require.Greater(t, details.RetryAfter(), time.Duration(0), "should show retry after when denied")

		// Consume some tokens
		require.True(t, limiters.allowN("test", now, 3))

		// Peek should now show reduced availability
		allowed, details = limiters.peekNWithDetails("test", now, 2)
		require.True(t, allowed, "should allow peeking remaining tokens")
		require.Equal(t, int64(2), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed())
		require.Equal(t, int64(2), details.TokensRemaining(), "should show remaining tokens after consumption")

		allowed, details = limiters.peekNWithDetails("test", now, 3)
		require.False(t, allowed, "should not allow peeking more than remaining")
		require.Equal(t, int64(3), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed())
		require.Equal(t, int64(2), details.TokensRemaining(), "should show actual remaining tokens")
		// requested 3, 2 are available, so wait for 1
		require.Equal(t, details.RetryAfter(), limit.DurationPerToken(), "should show retry after when denied")
	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		// Create two limiters with different limits
		limit1 := NewLimit(3, time.Second) // 3 per second
		limit2 := NewLimit(5, time.Minute) // 5 per minute

		limiter1 := NewLimiter(keyFunc, limit1)
		limiter2 := NewLimiter(keyFunc, limit2)
		limiters := Combine(limiter1, limiter2)

		now := bnow()

		// Should allow peeking up to the most restrictive limit (3)
		allowed, details := limiters.peekNWithDetails("test", now, 3)
		{
			require.True(t, allowed, "should allow peeking up to most restrictive limit")
			require.Equal(t, int64(3), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(3), details.TokensRemaining(), "should show minimum remaining across limiters")
			require.Equal(t, time.Duration(0), details.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, details = limiters.peekNWithDetails("test", now, 4)
		{
			require.False(t, allowed, "should not allow peeking beyond most restrictive limit")
			require.Equal(t, int64(4), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(3), details.TokensRemaining(), "should show minimum remaining across limiters")
			// 3 available, 4 requested, so wait for 1. rounding factor for rounding.
			rounding := time.Nanosecond
			require.Equal(t, limit1.DurationPerToken(), details.RetryAfter()+rounding, "should show retry after when denied")
		}
		// Consume tokens from both limiters
		require.True(t, limiters.allowN("test", now, 2))

		// Peek should now show reduced availability
		allowed, details = limiters.peekNWithDetails("test", now, 1)
		{
			require.True(t, allowed, "should allow peeking remaining tokens")
			require.Equal(t, int64(1), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(1), details.TokensRemaining(), "should show minimum remaining across limiters")
		}

		allowed, details = limiters.peekNWithDetails("test", now, 2)
		{
			require.False(t, allowed, "should not allow peeking more than remaining")
			require.Equal(t, int64(2), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(1), details.TokensRemaining(), "should show minimum remaining across limiters")
			// per-second exceeded by 1, so wait for 1
			rounding := time.Nanosecond
			require.Equal(t, limit1.DurationPerToken(), details.RetryAfter()+rounding, "should show retry after when denied")
		}
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

		now := bnow()

		// Should allow peeking up to the most restrictive limit (3)
		allowed, details := limiters.peekNWithDetails("test", now, 3)
		{
			require.True(t, allowed, "should allow peeking up to most restrictive limit")
			require.Equal(t, int64(3), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(3), details.TokensRemaining(), "should show minimum remaining across limiters")
			require.Equal(t, time.Duration(0), details.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, details = limiters.peekNWithDetails("test", now, 4)
		{
			require.False(t, allowed, "should not allow peeking beyond most restrictive limit")
			require.Equal(t, int64(4), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(3), details.TokensRemaining(), "should show minimum remaining across limiters")
			// 4 requested, 3 available per second, so wait for 1
			rounding := time.Nanosecond
			require.Equal(t, limit1.DurationPerToken(), details.RetryAfter()+rounding, "should show retry after when denied")
		}

		// Consume tokens from both limiters
		require.True(t, limiters.allowN("test", now, 2))

		// Peek should now show reduced availability
		allowed, details = limiters.peekNWithDetails("test", now, 1)
		{
			require.True(t, allowed, "should allow peeking remaining tokens")
			require.Equal(t, int64(1), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(1), details.TokensRemaining(), "should show minimum remaining across limiters")
			require.Equal(t, time.Duration(0), details.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, details = limiters.peekNWithDetails("test", now, 2)
		{
			require.False(t, allowed, "should not allow peeking more than remaining")
			require.Equal(t, int64(2), details.TokensRequested())
			require.Equal(t, int64(0), details.TokensConsumed())
			require.Equal(t, int64(1), details.TokensRemaining(), "should show minimum remaining across limiters")
			// per-second exceeded by 1, so wait for 1
			rounding := time.Nanosecond
			require.Equal(t, limit1.DurationPerToken(), details.RetryAfter()+rounding, "should show retry after when denied")
		}
	})

	t.Run("PublicAPI", func(t *testing.T) {
		// Test the public API methods
		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		// Test PeekWithDetails (single token)
		allowed, details := limiters.PeekWithDetails("test")
		require.True(t, allowed, "PeekWithDetails should return true when tokens available")
		require.Equal(t, int64(1), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed(), "peek never consumes tokens")
		require.Equal(t, int64(5), details.TokensRemaining(), "should show all tokens remaining")
		require.Equal(t, time.Duration(0), details.RetryAfter(), "should show 0 retry after when available")

		// Test PeekNWithDetails (multiple tokens)
		allowed, details = limiters.PeekNWithDetails("test", 3)
		require.True(t, allowed, "PeekNWithDetails should return true for 3 tokens")
		require.Equal(t, int64(3), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed())
		require.Equal(t, int64(5), details.TokensRemaining())
		require.Equal(t, time.Duration(0), details.RetryAfter(), "should show 0 retry after when available")

		allowed, details = limiters.PeekNWithDetails("test", 6)
		require.False(t, allowed, "PeekNWithDetails should return false for 6 tokens when limit is 5")
		require.Equal(t, int64(6), details.TokensRequested())
		require.Equal(t, int64(0), details.TokensConsumed())
		require.Equal(t, int64(5), details.TokensRemaining())
		require.Equal(t, limit.DurationPerToken(), details.RetryAfter(), "should show retry after when denied")

		// Peeks should not consume tokens - verify by consuming with allow
		for i := range 5 {
			allowed := limiters.allowN("test", bnow(), 1)
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
			now := bnow()

			const concurrency = 20

			// All concurrent peeks should succeed when bucket is full
			results := make([]bool, concurrency)
			detailsResults := make([]Details[string, string], concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index], detailsResults[index] = limiters.PeekWithDetails("test") // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed since peek doesn't consume tokens
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed when bucket is full", i)
				require.Equal(t, int64(1), detailsResults[i].TokensRequested())
				require.Equal(t, int64(0), detailsResults[i].TokensConsumed())
				require.Equal(t, int64(8), detailsResults[i].TokensRemaining())
			}

			// Exhaust the bucket with allow calls
			for i := range 8 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "allow call %d should succeed", i)
			}

			// All concurrent peeks should now fail
			results2 := make([]bool, concurrency)
			detailsResults2 := make([]Details[string, string], concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index], detailsResults2[index] = limiters.PeekWithDetails("test") // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should fail since bucket is exhausted
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d should fail when bucket is exhausted", i)
				require.Equal(t, int64(1), detailsResults2[i].TokensRequested())
				require.Equal(t, int64(0), detailsResults2[i].TokensConsumed())
				require.Equal(t, int64(0), detailsResults2[i].TokensRemaining())
				require.Greater(t, detailsResults2[i].RetryAfter(), time.Duration(0), "should show retry after when denied")
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

			now := bnow()
			const concurrency = 15

			// All concurrent peeks should succeed when buckets are full
			results := make([]bool, concurrency)
			detailsResults := make([]Details[string, string], concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index], detailsResults[index] = limiters.PeekNWithDetails("test", 1) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed with multiple limiters", i)
				require.Equal(t, int64(1), detailsResults[i].TokensRequested())
				require.Equal(t, int64(0), detailsResults[i].TokensConsumed())
				require.Equal(t, int64(4), detailsResults[i].TokensRemaining(), "should show minimum remaining across limiters")
			}

			// Exhaust the more restrictive limiter (limit1)
			for i := range 4 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "allow call %d should succeed", i)
			}

			// All concurrent peeks should now fail (limited by limiter1)
			results2 := make([]bool, concurrency)
			detailsResults2 := make([]Details[string, string], concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index], detailsResults2[index] = limiters.PeekNWithDetails("test", 1) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should fail since limiter1 is exhausted
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d should fail when most restrictive limiter is exhausted", i)
				require.Equal(t, int64(1), detailsResults2[i].TokensRequested())
				require.Equal(t, int64(0), detailsResults2[i].TokensConsumed())
				require.Equal(t, int64(0), detailsResults2[i].TokensRemaining())
				require.Greater(t, detailsResults2[i].RetryAfter(), time.Duration(0), "should show retry after when denied")
			}

			// Test concurrent peeks after refill
			futureTime := now.Add(time.Second)
			results3 := make([]bool, concurrency)
			detailsResults3 := make([]Details[string, string], concurrency)

			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results3[index], detailsResults3[index] = limiters.peekNWithDetails("test", futureTime, 1) // Mix public/private for thorough testing
				}(i)
			}
			wg.Wait()

			// All peeks should succeed after refill
			for i, result := range results3 {
				require.True(t, result, "concurrent peek %d should succeed after refill", i)
				require.Equal(t, int64(1), detailsResults3[i].TokensRequested())
				require.Equal(t, int64(0), detailsResults3[i].TokensConsumed())
				require.Equal(t, int64(4), detailsResults3[i].TokensRemaining(), "should show minimum remaining across limiters")
			}
		})

		t.Run("PeekDoesNotMutate", func(t *testing.T) {
			t.Parallel()

			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			limiters := Combine(limiter)

			now := bnow()
			const concurrency = 50 // High concurrency to stress test

			// Run many concurrent peeks
			results := make([]bool, concurrency)
			detailsResults := make([]Details[string, string], concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index], detailsResults[index] = limiters.PeekWithDetails("test") // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed", i)
				require.Equal(t, int64(1), detailsResults[i].TokensRequested())
				require.Equal(t, int64(0), detailsResults[i].TokensConsumed())
				require.Equal(t, int64(5), detailsResults[i].TokensRemaining())
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

			now := bnow()
			const concurrency = 20

			// Test concurrent peeks for multiple tokens
			results := make([]bool, concurrency)
			detailsResults := make([]Details[string, string], concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					// Each peek asks for 3 tokens
					results[index], detailsResults[index] = limiters.PeekNWithDetails("test", 3) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed since bucket has 10 tokens
			for i, result := range results {
				require.True(t, result, "concurrent peek %d for 3 tokens should succeed", i)
				require.Equal(t, int64(3), detailsResults[i].TokensRequested())
				require.Equal(t, int64(0), detailsResults[i].TokensConsumed())
				require.Equal(t, int64(10), detailsResults[i].TokensRemaining())
			}

			// Consume 8 tokens, leaving only 2
			allowed := limiters.allowN("test", now, 8)
			require.True(t, allowed, "should allow consuming 8 tokens")

			// Now concurrent peeks for 3 tokens should all fail
			results2 := make([]bool, concurrency)
			detailsResults2 := make([]Details[string, string], concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index], detailsResults2[index] = limiters.PeekNWithDetails("test", 3) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should fail since only 2 tokens remain
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d for 3 tokens should fail when only 2 remain", i)
				require.Equal(t, int64(3), detailsResults2[i].TokensRequested())
				require.Equal(t, int64(0), detailsResults2[i].TokensConsumed())
				require.Equal(t, int64(2), detailsResults2[i].TokensRemaining())
				require.Greater(t, detailsResults2[i].RetryAfter(), time.Duration(0), "should show retry after when denied")
			}

			// But peeks for 2 tokens should succeed
			results3 := make([]bool, concurrency)
			detailsResults3 := make([]Details[string, string], concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results3[index], detailsResults3[index] = limiters.PeekNWithDetails("test", 2) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks for 2 tokens should succeed
			for i, result := range results3 {
				require.True(t, result, "concurrent peek %d for 2 tokens should succeed", i)
				require.Equal(t, int64(2), detailsResults3[i].TokensRequested())
				require.Equal(t, int64(0), detailsResults3[i].TokensConsumed())
				require.Equal(t, int64(2), detailsResults3[i].TokensRemaining())
			}
		})
	})
}

func TestLimiters_PeekNWithDebug(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return input
	}

	t.Run("NoLimiters", func(t *testing.T) {
		limiters := Combine[string, string]()
		allowed, debugs := limiters.PeekNWithDebug("test", 1)
		require.True(t, allowed, "should allow when no limiters")
		require.Empty(t, debugs, "should have empty debug info when no limiters")

		allowed, debugs = limiters.PeekNWithDebug("test", 100)
		require.True(t, allowed, "should allow any amount when no limiters")
		require.Empty(t, debugs, "should have empty debug info when no limiters")
	})

	t.Run("SingleLimiter", func(t *testing.T) {
		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		now := bnow()

		// Should allow peeking up to the limit
		allowed, debugs := limiters.peekNWithDebug("test", now, 5)
		require.True(t, allowed, "should allow peeking up to limit")
		require.Len(t, debugs, 1, "should have debug info for single limit")
		require.Equal(t, int64(5), debugs[0].TokensRequested())
		require.Equal(t, int64(0), debugs[0].TokensConsumed(), "peek never consumes tokens")
		require.Equal(t, int64(5), debugs[0].TokensRemaining(), "should show all tokens remaining")
		require.Equal(t, time.Duration(0), debugs[0].RetryAfter(), "should show 0 retry after when available")

		allowed, debugs = limiters.peekNWithDebug("test", now, 6)
		require.False(t, allowed, "should not allow peeking beyond limit")
		require.Len(t, debugs, 1, "should have debug info for single limit")
		require.Equal(t, int64(6), debugs[0].TokensRequested())
		require.Equal(t, int64(0), debugs[0].TokensConsumed(), "peek never consumes tokens")
		require.Equal(t, int64(5), debugs[0].TokensRemaining(), "should show current remaining tokens")
		// 5 available, 6 requested, so wait for 1
		require.Equal(t, limit.DurationPerToken(), debugs[0].RetryAfter(), "RetryAfter should be duration per token for 1 additional token needed")

		// Consume some tokens
		require.True(t, limiters.allowN("test", now, 3))

		// Peek should now show reduced availability
		allowed, debugs = limiters.peekNWithDebug("test", now, 2)
		{
			d0 := debugs[0]
			require.True(t, allowed, "should allow peeking remaining tokens")
			require.Len(t, debugs, 1, "should have debug info for single limit")
			require.Equal(t, int64(2), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(2), d0.TokensRemaining(), "should show remaining tokens after consumption")
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, debugs = limiters.peekNWithDebug("test", now, 3)
		{
			d0 := debugs[0]
			require.False(t, allowed, "should not allow peeking more than remaining")
			require.Len(t, debugs, 1, "should have debug info for single limit")
			require.Equal(t, int64(3), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(2), d0.TokensRemaining(), "should show actual remaining tokens")
			// 2 available, 3 requested, so wait for 1
			require.Equal(t, limit.DurationPerToken(), debugs[0].RetryAfter(), "RetryAfter should be duration per token for 1 additional token needed")
		}

	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		// Create two limiters with different limits
		limit1 := NewLimit(3, time.Second) // 3 per second
		limit2 := NewLimit(5, time.Minute) // 5 per minute

		limiter1 := NewLimiter(keyFunc, limit1)
		limiter2 := NewLimiter(keyFunc, limit2)
		limiters := Combine(limiter1, limiter2)

		now := bnow()

		// Should allow peeking up to the most restrictive limit (3)
		allowed, debugs := limiters.peekNWithDebug("test", now, 3)
		{
			require.True(t, allowed, "should allow peeking up to most restrictive limit")
			require.Len(t, debugs, 2, "should have debug info for both limiters")

			d0 := debugs[0]
			require.True(t, d0.Allowed(), "should allow peeking up to most restrictive limit")
			require.Equal(t, int64(3), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(3), d0.TokensRemaining(), "should show limit1 remaining tokens")
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")

			d1 := debugs[1]
			require.True(t, d1.Allowed(), "should allow peeking up to least restrictive limit")
			require.Equal(t, int64(3), d1.TokensRequested())
			require.Equal(t, int64(0), d1.TokensConsumed())
			require.Equal(t, int64(5), d1.TokensRemaining(), "should show limit2 remaining tokens")
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, debugs = limiters.peekNWithDebug("test", now, 4)
		{
			require.False(t, allowed, "should not allow peeking beyond most restrictive limit")
			require.Len(t, debugs, 2, "should have debug info for both limiters")

			d0 := debugs[0]
			require.False(t, d0.Allowed(), "should not allow peeking beyond most restrictive limit")
			require.Equal(t, int64(4), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(3), d0.TokensRemaining(), "should show limit1 remaining tokens")
			// 3 available, 4 requested, so wait for 1. fudge factor for rounding.
			rounding := time.Nanosecond
			require.Equal(t, limit1.DurationPerToken(), d0.RetryAfter()+rounding, "should show retry after when denied")

			d1 := debugs[1]
			require.True(t, d1.Allowed(), "should allow peeking up to least restrictive limit")
			require.Equal(t, int64(4), d1.TokensRequested())
			require.Equal(t, int64(0), d1.TokensConsumed())
			require.Equal(t, int64(5), d1.TokensRemaining(), "should show limit2 remaining tokens")
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, debugs = limiters.peekNWithDebug("test", now, 6)
		{
			require.False(t, allowed, "should not allow peeking beyond most restrictive limit")
			require.Len(t, debugs, 2, "should have debug info for both limiters")

			d0 := debugs[0]
			require.False(t, d0.Allowed(), "should not allow peeking beyond most restrictive limit")
			require.Equal(t, int64(6), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(3), d0.TokensRemaining(), "should show limit1 remaining tokens")
			// 3 available, 6 requested, so wait for 3
			rounding := time.Nanosecond
			require.Equal(t, 3*limit1.DurationPerToken(), d0.RetryAfter()+rounding, "should show retry after when denied")

			d1 := debugs[1]
			require.False(t, d1.Allowed(), "should not allow peeking beyond least restrictive limit")
			require.Equal(t, int64(6), d1.TokensRequested())
			require.Equal(t, int64(0), d1.TokensConsumed())
			require.Equal(t, int64(5), d1.TokensRemaining(), "should show limit2 remaining tokens")
			// need to wait for one more token
			require.Equal(t, limit2.DurationPerToken(), d1.RetryAfter(), "should show retry after when denied")
		}

		// Consume tokens from both limiters
		require.True(t, limiters.allowN("test", now, 2))

		// Peek should now show reduced availability
		allowed, debugs = limiters.peekNWithDebug("test", now, 1)
		{
			require.True(t, allowed, "should allow peeking remaining tokens")
			require.Len(t, debugs, 2, "should have debug info for both limiters")

			d0 := debugs[0]
			require.True(t, d0.Allowed(), "should allow peeking up to most restrictive limit")
			require.Equal(t, int64(1), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(1), d0.TokensRemaining(), "should show limit1 remaining tokens")
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")

			d1 := debugs[1]
			require.True(t, d1.Allowed(), "should allow peeking up to least restrictive limit")
			require.Equal(t, int64(1), d1.TokensRequested())
			require.Equal(t, int64(0), d1.TokensConsumed())
			require.Equal(t, int64(3), d1.TokensRemaining(), "should show limit2 remaining tokens")
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, debugs = limiters.peekNWithDebug("test", now, 2)
		{
			require.False(t, allowed, "should not allow peeking more than remaining")
			require.Len(t, debugs, 2, "should have debug info for both limiters")

			d0 := debugs[0]
			require.False(t, d0.Allowed(), "should not allow peeking beyond most restrictive limit")
			require.Equal(t, int64(2), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(1), d0.TokensRemaining(), "should show limit1 remaining tokens")
			// 1 available, 2 requested, so wait for 1
			rounding := time.Nanosecond
			require.Equal(t, limit1.DurationPerToken(), d0.RetryAfter()+rounding, "should show retry after when denied")

			d1 := debugs[1]
			require.True(t, d1.Allowed(), "should allow peeking up to least restrictive limit")
			require.Equal(t, int64(2), d1.TokensRequested())
			require.Equal(t, int64(0), d1.TokensConsumed())
			require.Equal(t, int64(3), d1.TokensRemaining(), "should show limit2 remaining tokens")
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "should show 0 retry after when available")
		}
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

		now := bnow()

		// Should allow peeking up to the most restrictive limit (3)
		allowed, debugs := limiters.peekNWithDebug("test", now, 3)
		{
			require.True(t, allowed, "should allow peeking up to most restrictive limit")
			require.Len(t, debugs, 2, "should have debug info for both limiters")

			d0 := debugs[0]
			require.Equal(t, int64(3), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(3), d0.TokensRemaining(), "should show limit1 remaining tokens")
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")

			d1 := debugs[1]
			require.True(t, d1.Allowed(), "should allow peeking up to least restrictive limit")
			require.Equal(t, int64(3), d1.TokensRequested())
			require.Equal(t, int64(0), d1.TokensConsumed())
			require.Equal(t, int64(5), d1.TokensRemaining(), "should show limit2 remaining tokens")
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, debugs = limiters.peekNWithDebug("test", now, 4)
		{
			// per-second was exceeded
			require.False(t, allowed, "should not allow peeking beyond most restrictive limit")
			require.Len(t, debugs, 2, "should have debug info for both limiters")

			d0 := debugs[0]
			require.Equal(t, int64(4), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(3), d0.TokensRemaining(), "should show limit1 remaining tokens")
			// 3 available, 4 requested, so wait for 1. fudge factor for rounding.
			rounding := time.Nanosecond
			require.Equal(t, limit1.DurationPerToken(), d0.RetryAfter()+rounding, "should show retry after when denied")

			// per-minute was not exceeded
			d1 := debugs[1]
			require.True(t, d1.Allowed(), "should not allow peeking beyond least restrictive limit")
			require.Equal(t, int64(4), d1.TokensRequested())
			require.Equal(t, int64(0), d1.TokensConsumed())
			require.Equal(t, int64(5), d1.TokensRemaining(), "should show limit2 remaining tokens")
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "should show 0 retry after when available")
		}

		// Consume tokens from both limiters
		require.True(t, limiters.allowN("test", now, 2))

		// Peek should now show reduced availability
		allowed, debugs = limiters.peekNWithDebug("test", now, 1)
		{
			require.True(t, allowed, "should allow peeking remaining tokens")
			require.Len(t, debugs, 2, "should have debug info for both limiters")
			d0 := debugs[0]
			require.Equal(t, int64(1), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(1), d0.TokensRemaining(), "should show limit1 remaining tokens")
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")

			d1 := debugs[1]
			require.True(t, d1.Allowed(), "should allow peeking up to least restrictive limit")
			require.Equal(t, int64(1), d1.TokensRequested())
			require.Equal(t, int64(0), d1.TokensConsumed())
			require.Equal(t, int64(3), d1.TokensRemaining(), "should show limit2 remaining tokens")
			require.Equal(t, time.Duration(0), d1.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, debugs = limiters.peekNWithDebug("test", now, 2)
		{
			require.False(t, allowed, "should not allow peeking more than remaining")
			require.Len(t, debugs, 2, "should have debug info for both limiters")

			d0 := debugs[0]
			require.Equal(t, int64(2), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(1), d0.TokensRemaining(), "should show limit1 remaining tokens")
			rounding := time.Nanosecond
			require.Equal(t, limit1.DurationPerToken(), d0.RetryAfter()+rounding, "should show retry after when denied")
		}
	})

	t.Run("PublicAPI", func(t *testing.T) {
		// Test the public API methods
		limit := NewLimit(5, time.Second)
		limiter := NewLimiter(keyFunc, limit)
		limiters := Combine(limiter)

		// Test PeekWithDebug (single token)
		allowed, debugs := limiters.PeekWithDebug("test")
		{
			require.True(t, allowed, "PeekWithDebug should return true when tokens available")
			require.Len(t, debugs, 1, "should have debug info for single limit")

			d0 := debugs[0]
			require.Equal(t, int64(1), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed(), "peek never consumes tokens")
			require.Equal(t, int64(5), d0.TokensRemaining(), "should show all tokens remaining")
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")
		}
		// Test PeekNWithDebug (multiple tokens)
		allowed, debugs = limiters.PeekNWithDebug("test", 3)
		{
			require.True(t, allowed, "PeekNWithDebug should return true for 3 tokens")
			require.Len(t, debugs, 1, "should have debug info for single limit")

			d0 := debugs[0]
			require.Equal(t, int64(3), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(5), d0.TokensRemaining())
			require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")
		}

		allowed, debugs = limiters.PeekNWithDebug("test", 6)
		{
			require.False(t, allowed, "PeekNWithDebug should return false for 6 tokens when limit is 5")
			require.Len(t, debugs, 1, "should have debug info for single limit")

			d0 := debugs[0]
			require.Equal(t, int64(6), d0.TokensRequested())
			require.Equal(t, int64(0), d0.TokensConsumed())
			require.Equal(t, int64(5), d0.TokensRemaining())
			// 5 available, 6 requested, so wait for 1
			require.Equal(t, limit.DurationPerToken(), d0.RetryAfter(), "RetryAfter should be duration per token for 1 additional token needed")
		}
		// Peeks should not consume tokens - verify by consuming with allow
		for i := range 5 {
			allowed := limiters.allowN("test", bnow(), 1)
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
			now := bnow()

			const concurrency = 20

			// All concurrent peeks should succeed when bucket is full
			results := make([]bool, concurrency)
			debugsResults := make([][]Debug[string, string], concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index], debugsResults[index] = limiters.PeekWithDebug("test") // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed since peek doesn't consume tokens
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed when bucket is full", i)
				require.Len(t, debugsResults[i], 1, "should have debug info for single limit")

				d0 := debugsResults[i][0]
				require.Equal(t, int64(1), d0.TokensRequested())
				require.Equal(t, int64(0), d0.TokensConsumed())
				require.Equal(t, int64(8), d0.TokensRemaining())
				require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")
			}

			// Exhaust the bucket with allow calls
			for i := range 8 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "allow call %d should succeed", i)
			}

			// All concurrent peeks should now fail
			results2 := make([]bool, concurrency)
			debugsResults2 := make([][]Debug[string, string], concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index], debugsResults2[index] = limiters.PeekWithDebug("test") // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should fail since bucket is exhausted
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d should fail when bucket is exhausted", i)
				require.Len(t, debugsResults2[i], 1, "should have debug info for single limit")

				d0 := debugsResults2[i][0]
				require.Equal(t, int64(1), d0.TokensRequested())
				require.Equal(t, int64(0), d0.TokensConsumed())
				require.Equal(t, int64(0), d0.TokensRemaining())
				require.Greater(t, d0.RetryAfter(), time.Duration(0), "should show retry after when denied")
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

			now := bnow()
			const concurrency = 15

			// All concurrent peeks should succeed when buckets are full
			results := make([]bool, concurrency)
			debugsResults := make([][]Debug[string, string], concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index], debugsResults[index] = limiters.PeekNWithDebug("test", 1) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed with multiple limiters", i)
				require.Len(t, debugsResults[i], 2, "should have debug info for both limiters")

				d0 := debugsResults[i][0]
				require.Equal(t, int64(1), d0.TokensRequested())
				require.Equal(t, int64(0), d0.TokensConsumed())
				require.Equal(t, int64(4), d0.TokensRemaining(), "should show minimum remaining across limiters")
				require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")
			}

			// Exhaust the more restrictive limiter (limit1)
			for i := range 4 {
				allowed := limiters.allowN("test", now, 1)
				require.True(t, allowed, "allow call %d should succeed", i)
			}

			// All concurrent peeks should now fail (limited by limiter1)
			results2 := make([]bool, concurrency)
			debugsResults2 := make([][]Debug[string, string], concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index], debugsResults2[index] = limiters.PeekNWithDebug("test", 1) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should fail since limiter1 is exhausted
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d should fail when most restrictive limiter is exhausted", i)
				require.Len(t, debugsResults2[i], 2, "should have debug info for both limiters")

				d0 := debugsResults2[i][0]
				require.Equal(t, int64(1), d0.TokensRequested())
				require.Equal(t, int64(0), d0.TokensConsumed())
				require.Equal(t, int64(0), d0.TokensRemaining())
				require.Greater(t, d0.RetryAfter(), time.Duration(0), "should show retry after when denied")
			}

			// Test concurrent peeks after refill
			futureTime := now.Add(time.Second)
			results3 := make([]bool, concurrency)
			debugsResults3 := make([][]Debug[string, string], concurrency)

			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results3[i], debugsResults3[i] = limiters.peekNWithDebug("test", futureTime, 1) // Mix public/private for thorough testing
				}(i)
			}
			wg.Wait()

			// All peeks should succeed after refill
			for i, result := range results3 {
				require.True(t, result, "concurrent peek %d should succeed after refill", i)
				require.Len(t, debugsResults3[i], 2, "should have debug info for both limiters")

				d0 := debugsResults3[i][0]
				require.Equal(t, int64(1), d0.TokensRequested())
				require.Equal(t, int64(0), d0.TokensConsumed())
				require.Equal(t, int64(4), d0.TokensRemaining(), "should show minimum remaining across limiters")
				require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")
			}
		})

		t.Run("PeekDoesNotMutate", func(t *testing.T) {
			t.Parallel()

			limit := NewLimit(5, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			limiters := Combine(limiter)

			now := bnow()
			const concurrency = 50 // High concurrency to stress test

			// Run many concurrent peeks
			results := make([]bool, concurrency)
			debugsResults := make([][]Debug[string, string], concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results[index], debugsResults[index] = limiters.PeekWithDebug("test") // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed
			for i, result := range results {
				require.True(t, result, "concurrent peek %d should succeed", i)
				require.Len(t, debugsResults[i], 1, "should have debug info for single limit")

				d0 := debugsResults[i][0]
				require.Equal(t, int64(1), d0.TokensRequested())
				require.Equal(t, int64(0), d0.TokensConsumed())
				require.Equal(t, int64(5), d0.TokensRemaining())
				require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")
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

			now := bnow()
			const concurrency = 20

			// Test concurrent peeks for multiple tokens
			results := make([]bool, concurrency)
			debugsResults := make([][]Debug[string, string], concurrency)

			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					// Each peek asks for 3 tokens
					results[index], debugsResults[index] = limiters.PeekNWithDebug("test", 3) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should succeed since bucket has 10 tokens
			for i, result := range results {
				require.True(t, result, "concurrent peek %d for 3 tokens should succeed", i)
				require.Len(t, debugsResults[i], 1, "should have debug info for single limit")

				d0 := debugsResults[i][0]
				require.Equal(t, int64(3), d0.TokensRequested())
				require.Equal(t, int64(0), d0.TokensConsumed())
				require.Equal(t, int64(10), d0.TokensRemaining())
				require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")
			}

			// Consume 8 tokens, leaving only 2
			allowed := limiters.allowN("test", now, 8)
			require.True(t, allowed, "should allow consuming 8 tokens")

			// Now concurrent peeks for 3 tokens should all fail
			results2 := make([]bool, concurrency)
			debugsResults2 := make([][]Debug[string, string], concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results2[index], debugsResults2[index] = limiters.PeekNWithDebug("test", 3) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks should fail since only 2 tokens remain
			for i, result := range results2 {
				require.False(t, result, "concurrent peek %d for 3 tokens should fail when only 2 remain", i)
				require.Len(t, debugsResults2[i], 1, "should have debug info for single limit")

				d0 := debugsResults2[i][0]
				require.Equal(t, int64(3), d0.TokensRequested())
				require.Equal(t, int64(0), d0.TokensConsumed())
				require.Equal(t, int64(2), d0.TokensRemaining())
				require.Greater(t, d0.RetryAfter(), time.Duration(0), "should show retry after when denied")
			}

			// But peeks for 2 tokens should succeed
			results3 := make([]bool, concurrency)
			debugsResults3 := make([][]Debug[string, string], concurrency)
			for i := range concurrency {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					results3[i], debugsResults3[i] = limiters.PeekNWithDebug("test", 2) // Use public API
				}(i)
			}
			wg.Wait()

			// All peeks for 2 tokens should succeed
			for i, result := range results3 {
				require.True(t, result, "concurrent peek %d for 2 tokens should succeed", i)
				require.Len(t, debugsResults3[i], 1, "should have debug info for single limit")

				d0 := debugsResults3[i][0]
				require.Equal(t, int64(2), d0.TokensRequested())
				require.Equal(t, int64(0), d0.TokensConsumed())
				require.Equal(t, int64(2), d0.TokensRemaining())
				require.Equal(t, time.Duration(0), d0.RetryAfter(), "should show 0 retry after when available")
			}
		})
	})
}
