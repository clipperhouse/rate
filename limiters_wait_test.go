package rate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/clipperhouse/ntime"
	"github.com/stretchr/testify/require"
)

func TestLimiters_Wait(t *testing.T) {
	t.Parallel()

	t.Run("SingleBucket", func(t *testing.T) {
		t.Parallel()
		keyFunc := func(input string) string {
			return input
		}
		// Create two limiters with different limits
		limit1 := NewLimit(2, 100*time.Millisecond) // 2 per 100ms (more restrictive)
		limit2 := NewLimit(5, 100*time.Millisecond) // 5 per 100ms
		limiter1 := NewLimiter(keyFunc, limit1)
		limiter2 := NewLimiter(keyFunc, limit2)
		limiters := Combine(limiter1, limiter2)

		executionTime := ntime.Now()

		// Consume all tokens from the more restrictive limiter
		for range limit1.count {
			ok := limiters.allowN("test", executionTime, 1)
			require.True(t, ok, "should allow initial tokens")
		}

		// Should not allow immediately (limiter1 exhausted)
		ok := limiters.allowN("test", executionTime, 1)
		require.False(t, ok, "should not allow when limiter1 tokens exhausted")

		// Test 1: Wait for 1 token with no cancellation
		{
			ctx := &testContext{
				done: make(chan struct{}), // never closes
			}

			allow, details, err := limiters.waitNWithDetails(ctx, "test", executionTime, 1)
			require.True(t, allow, "should acquire 1 token after waiting")
			require.NotZero(t, details, "should return details")
			require.NoError(t, err, "should not return error")

			// We waited
			executionTime = executionTime.Add(limit1.durationPerToken)
		}

		// Should not allow again immediately after wait
		ok = limiters.allowN("test", executionTime, 1)
		require.False(t, ok, "should not allow again immediately after wait")

		// Test 2: Wait for 1 token with immediate cancellation
		{
			// Context that's immediately cancelled (closed)
			done := make(chan struct{})
			ctx := &testContext{
				done: done,
			}
			close(done)

			allow, details, err := limiters.waitNWithDetails(ctx, "test", executionTime, 1)
			require.False(t, allow, "should not acquire 1 token if context is cancelled immediately")
			require.NotZero(t, details, "should return details even on cancellation")
			require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
		}

		// Test 3: Wait for multiple tokens (limit1.count) with no cancellation
		{
			// Wait for bucket to refill enough tokens
			executionTime = executionTime.Add(limit1.period)

			ctx := &testContext{
				done: make(chan struct{}), // never closes
			}

			allow, details, err := limiters.waitNWithDetails(ctx, "test", executionTime, limit1.count)
			require.True(t, allow, "should acquire %d tokens after waiting", limit1.count)
			require.NotZero(t, details, "should return details")
			require.NoError(t, err, "should not return error")
		}

		// Test 4: Wait for multiple tokens with immediate cancellation
		{
			// Wait for bucket to refill enough tokens
			executionTime = executionTime.Add(limit1.durationPerToken * time.Duration(limit1.count))

			// Consume all tokens to exhaust the bucket again
			for range limit1.count {
				ok := limiters.allowN("test", executionTime, 1)
				require.True(t, ok, "should allow tokens to exhaust bucket")
			}

			// Context that's immediately cancelled (closed)
			done := make(chan struct{})
			ctx := &testContext{
				done: done,
			}
			close(done)

			allow, details, err := limiters.waitNWithDetails(ctx, "test", executionTime, limit1.count)
			require.False(t, allow, "should not acquire %d tokens if context is cancelled immediately", limit1.count)
			require.NotZero(t, details, "should return details even on cancellation")
			require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
		}
	})

	t.Run("MultipleBuckets", func(t *testing.T) {
		t.Run("Serial", func(t *testing.T) {
			t.Parallel()
			keyFunc := func(input int) string {
				return fmt.Sprintf("test-bucket-%d", input)
			}
			const buckets = 3
			// Create two limiters with different limits
			limit1 := NewLimit(2, 100*time.Millisecond) // 2 per 100ms (more restrictive)
			limit2 := NewLimit(4, 100*time.Millisecond) // 4 per 100ms
			limiter1 := NewLimiter(keyFunc, limit1)
			limiter2 := NewLimiter(keyFunc, limit2)
			limiters := Combine(limiter1, limiter2)
			executionTime := ntime.Now()

			// Exhaust tokens for all buckets from the more restrictive limiter
			for bucketID := range buckets {
				for range limit1.count {
					allow := limiters.allowN(bucketID, executionTime, 1)
					require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
				}
			}

			// Should not allow immediately for any bucket (limiter1 exhausted)
			for bucketID := range buckets {
				allow := limiters.allowN(bucketID, executionTime, 1)
				require.False(t, allow, "should not allow when limiter1 tokens exhausted for bucket %d", bucketID)
			}

			// Wait for a token for each bucket
			for bucketID := range buckets {
				ctx := &testContext{
					done: make(chan struct{}), // never closes
				}

				allow, details, err := limiters.waitNWithDetails(ctx, bucketID, executionTime, 1)
				require.True(t, allow, "should acquire token after waiting for bucket %d", bucketID)
				require.NotZero(t, details, "should return details")
				require.NoError(t, err, "should not return error")
			}

			// We waited
			executionTime = executionTime.Add(limit1.durationPerToken)

			// Buckets should be empty again
			for bucketID := range buckets {
				ok := limiters.allowN(bucketID, executionTime, 1)
				require.False(t, ok, "should not allow again immediately after wait for bucket %d", bucketID)
			}
		})

		t.Run("Concurrent", func(t *testing.T) {
			// t.Parallel()
			// GitHub Actions are pretty resource constrained,
			// and this test is sensitive to timing.
			keyFunc := func(input int64) string {
				return fmt.Sprintf("test-bucket-%d", input)
			}
			const buckets int64 = 3
			// Create two limiters with different limits
			limit1 := NewLimit(2, 100*time.Millisecond) // 2 per 100ms (more restrictive)
			limit2 := NewLimit(4, 100*time.Millisecond) // 4 per 100ms
			limiter1 := NewLimiter(keyFunc, limit1)
			limiter2 := NewLimiter(keyFunc, limit2)
			limiters := Combine(limiter1, limiter2)
			executionTime := ntime.Now()

			// Exhaust tokens for all buckets from the more restrictive limiter
			for bucketID := range buckets {
				for range limit1.count {
					allow := limiters.allowN(bucketID, executionTime, 1)
					require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
				}
			}

			// Buckets should be empty (limiter1 exhausted)
			for bucketID := range buckets {
				allow := limiters.allowN(bucketID, executionTime, 1)
				require.False(t, allow, "should not allow when limiter1 tokens exhausted for bucket %d", bucketID)
			}

			// Test 1: Multiple goroutines with immediate cancellation
			{
				concurrency := buckets
				results := make([]bool, concurrency)

				// Context that's immediately cancelled (closed)
				done := make(chan struct{})
				ctx := &testContext{
					done: done,
				}
				close(done)

				// Start concurrent waits
				var wg sync.WaitGroup
				for i := range concurrency {
					wg.Add(1)
					go func(i int64) {
						defer wg.Done()
						allow, _, err := limiters.waitNWithDetails(ctx, i, executionTime, 1)
						require.False(t, allow, "should not acquire token due to immediate cancellation")
						require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
						results[i] = allow
					}(i)
				}
				wg.Wait()

				// All should fail because context is cancelled immediately
				for i, result := range results {
					require.False(t, result, "goroutine %d should not acquire token due to immediate cancellation", i)
				}
			}

			// Test 2: Multiple goroutines with delayed cancellation
			{
				keyFunc := func(input int64) int64 {
					return input
				}

				// Create two limiters with different limits
				limit1 := NewLimit(1, 200*time.Millisecond) // 1 per 200ms (more restrictive)
				limit2 := NewLimit(3, 200*time.Millisecond) // 3 per 200ms
				limiter1 := NewLimiter(keyFunc, limit1)
				limiter2 := NewLimiter(keyFunc, limit2)
				limiters := Combine(limiter1, limiter2)

				// Exhaust tokens from the more restrictive limiter
				for bucketID := range buckets {
					for range limit1.count {
						allow := limiters.allowN(bucketID, executionTime, 1)
						require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
					}
				}

				// We're trying to emulate a wait call
				// that starts waiting with a retry, but
				// is cancelled in the meantime.

				const buckets int64 = 3
				results := make([]bool, buckets)

				var wg sync.WaitGroup
				for bucketID := range buckets {
					wg.Add(1)

					done := make(chan struct{})
					ctx := &testContext{
						done: done,
					}

					go func(bucketID int64) {
						defer wg.Done()
						// Should start waiting, since the bucket is empty.
						// retryAfter should be ~200ms
						allow, _, err := limiters.waitNWithDetails(ctx, bucketID, executionTime, 1)
						require.False(t, allow, "should not acquire token due to cancellation")
						require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
						results[bucketID] = allow
					}(bucketID)

					// Cancel context after goroutine has started.
					// Originally, this had a delay, for a better test,
					// but it is flaky on GitHub Actions, presumably
					// because the runner is resource constrained.
					// Delay works fine locally on Mac M2.
					close(done)
				}
				wg.Wait()

				// All should fail because context is cancelled
				for i, result := range results {
					require.False(t, result, "goroutine %d should not acquire token due to cancellation", i)
				}
			}
		})
	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		t.Parallel()
		t.Run("SameKeyer", func(t *testing.T) {
			t.Parallel()

			keyFunc := func(input string) string {
				return fmt.Sprintf("bucket-%s", input)
			}

			// Create two limiters with different limits
			limit1 := NewLimit(3, 100*time.Millisecond) // 3 per 100ms (more restrictive)
			limit2 := NewLimit(5, 100*time.Millisecond) // 5 per 100ms
			limiter1 := NewLimiter(keyFunc, limit1)
			limiter2 := NewLimiter(keyFunc, limit2)

			// Create Limiters with both limiters
			limiters := Combine(limiter1, limiter2)

			executionTime := ntime.Now()

			// Consume all tokens from the more restrictive limiter
			for range limit1.count {
				ok := limiters.allowN("test", executionTime, 1)
				require.True(t, ok, "should allow initial tokens")
			}

			// Should not allow immediately (limiter1 exhausted)
			ok := limiters.allowN("test", executionTime, 1)
			require.False(t, ok, "should not allow when limiter1 tokens exhausted")

			// Test 1: Wait for 1 token with no cancellation
			{
				ctx := &testContext{
					done: make(chan struct{}), // never closes
				}

				allow, details, err := limiters.waitNWithDetails(ctx, "test", executionTime, 1)
				require.True(t, allow, "should acquire 1 token after waiting")
				require.NotZero(t, details, "should return details")
				require.NoError(t, err, "should not return error")

				// We waited
				executionTime = executionTime.Add(limit1.durationPerToken)
			}

			// Should not allow again immediately after wait
			ok = limiters.allowN("test", executionTime, 1)
			require.False(t, ok, "should not allow again immediately after wait")

			// Test 2: Wait for 1 token with immediate cancellation
			{
				// Context that's immediately cancelled (closed)
				done := make(chan struct{})
				ctx := &testContext{
					done: done,
				}
				close(done)

				allow, details, err := limiters.waitNWithDetails(ctx, "test", executionTime, 1)
				require.False(t, allow, "should not acquire 1 token if context is cancelled immediately")
				require.NotZero(t, details, "should return details even on cancellation")
				require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
			}
		})

		t.Run("DifferentKeyers", func(t *testing.T) {
			t.Parallel()

			// Create limiters with different key functions
			keyFunc1 := func(input string) string { return input + "-1" }
			keyFunc2 := func(input string) string { return input + "-2" }

			limit1 := NewLimit(2, 100*time.Millisecond)
			limit2 := NewLimit(3, 100*time.Millisecond)
			limiter1 := NewLimiter(keyFunc1, limit1)
			limiter2 := NewLimiter(keyFunc2, limit2)

			limiters := Combine(limiter1, limiter2)
			executionTime := ntime.Now()

			// Test that we can consume tokens from both keyers
			// The Combine function treats all limiters as a single unit
			// We should be able to consume up to the most restrictive limit (2) from each keyer
			for i := range 2 {
				ok := limiters.allowN("test", executionTime, 1)
				require.True(t, ok, "should allow token %d from keyer1", i+1)
			}

			// Should not allow more from keyer1 (limit1 exhausted)
			ok := limiters.allowN("test", executionTime, 1)
			require.False(t, ok, "should not allow when keyer1 limit exhausted")

			// Test: Wait for 1 token with no cancellation
			{
				ctx := &testContext{
					done: make(chan struct{}), // never closes
				}

				allow, details, err := limiters.waitNWithDetails(ctx, "test", executionTime, 1)
				require.True(t, allow, "should acquire 1 token after waiting")
				require.NotZero(t, details, "should return details")
				require.NoError(t, err, "should not return error")

				// We waited
				executionTime = executionTime.Add(limit1.durationPerToken)
			}

			// Should not allow again immediately after wait
			ok = limiters.allowN("test", executionTime, 1)
			require.False(t, ok, "should not allow again immediately after wait")
		})
	})
}

func TestLimiters_WaitWithDebug(t *testing.T) {
	t.Parallel()

	t.Run("SingleBucket", func(t *testing.T) {
		t.Parallel()
		keyFunc := func(input string) string {
			return input
		}
		// Create two limiters with different limits
		limit1 := NewLimit(2, 100*time.Millisecond) // 2 per 100ms (more restrictive)
		limit2 := NewLimit(5, 100*time.Millisecond) // 5 per 100ms
		limiter1 := NewLimiter(keyFunc, limit1)
		limiter2 := NewLimiter(keyFunc, limit2)
		limiters := Combine(limiter1, limiter2)

		executionTime := ntime.Now()

		// Consume all tokens from the more restrictive limiter
		for range limit1.count {
			ok := limiters.allowN("test", executionTime, 1)
			require.True(t, ok, "should allow initial tokens")
		}

		// Should not allow immediately (limiter1 exhausted)
		ok := limiters.allowN("test", executionTime, 1)
		require.False(t, ok, "should not allow when limiter1 tokens exhausted")

		// Test 1: Wait for 1 token with no cancellation
		{
			ctx := &testContext{
				done: make(chan struct{}), // never closes
			}

			allow, debugs, err := limiters.waitNWithDebug(ctx, "test", executionTime, 1)
			require.True(t, allow, "should acquire 1 token after waiting")
			require.NotNil(t, debugs, "should return debugs")
			require.NoError(t, err, "should not return error")

			// We waited
			executionTime = executionTime.Add(limit1.durationPerToken)
		}

		// Should not allow again immediately after wait
		ok = limiters.allowN("test", executionTime, 1)
		require.False(t, ok, "should not allow again immediately after wait")

		// Test 2: Wait for 1 token with immediate cancellation
		{
			// Context that's immediately cancelled (closed)
			done := make(chan struct{})
			ctx := &testContext{
				done: done,
			}
			close(done)

			allow, debugs, err := limiters.waitNWithDebug(ctx, "test", executionTime, 1)
			require.False(t, allow, "should not acquire 1 token if context is cancelled immediately")
			require.NotNil(t, debugs, "should return debugs even on cancellation")
			require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
		}

		// Test 3: Wait for multiple tokens (limit1.count) with no cancellation
		{
			// Wait for bucket to refill enough tokens
			executionTime = executionTime.Add(limit1.durationPerToken * time.Duration(limit1.count))

			ctx := &testContext{
				done: make(chan struct{}), // never closes
			}

			allow, debugs, err := limiters.waitNWithDebug(ctx, "test", executionTime, limit1.count)
			require.True(t, allow, "should acquire %d tokens after waiting", limit1.count)
			require.NotNil(t, debugs, "should return debugs")
			require.NoError(t, err, "should not return error")

			// We waited
			executionTime = executionTime.Add(limit1.durationPerToken)
		}

		// Test 4: Wait for multiple tokens with immediate cancellation
		{
			// Wait for bucket to refill enough tokens
			executionTime = executionTime.Add(limit1.durationPerToken * time.Duration(limit1.count))

			// Consume all tokens to exhaust the bucket again
			for range limit1.count {
				ok := limiters.allowN("test", executionTime, 1)
				require.True(t, ok, "should allow tokens to exhaust bucket")
			}

			// Context that's immediately cancelled (closed)
			done := make(chan struct{})
			ctx := &testContext{
				done: done,
			}
			close(done)

			allow, debugs, err := limiters.waitNWithDebug(ctx, "test", executionTime, limit1.count)
			require.False(t, allow, "should not acquire %d tokens if context is cancelled immediately", limit1.count)
			require.NotNil(t, debugs, "should return debugs even on cancellation")
			require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
		}
	})

	t.Run("MultipleBuckets", func(t *testing.T) {
		t.Run("Serial", func(t *testing.T) {
			t.Parallel()
			keyFunc := func(input int) string {
				return fmt.Sprintf("test-bucket-%d", input)
			}
			const buckets = 3
			// Create two limiters with different limits
			limit1 := NewLimit(2, 100*time.Millisecond) // 2 per 100ms (more restrictive)
			limit2 := NewLimit(4, 100*time.Millisecond) // 4 per 100ms
			limiter1 := NewLimiter(keyFunc, limit1)
			limiter2 := NewLimiter(keyFunc, limit2)
			limiters := Combine(limiter1, limiter2)
			executionTime := ntime.Now()

			// Exhaust tokens for all buckets from the more restrictive limiter
			for bucketID := range buckets {
				for range limit1.count {
					allow := limiters.allowN(bucketID, executionTime, 1)
					require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
				}
			}

			// Should not allow immediately for any bucket (limiter1 exhausted)
			for bucketID := range buckets {
				allow := limiters.allowN(bucketID, executionTime, 1)
				require.False(t, allow, "should not allow when limiter1 tokens exhausted for bucket %d", bucketID)
			}

			// Wait for a token for each bucket
			for bucketID := range buckets {
				ctx := &testContext{
					done: make(chan struct{}), // never closes
				}

				allow, debugs, err := limiters.waitNWithDebug(ctx, bucketID, executionTime, 1)
				require.True(t, allow, "should acquire token after waiting for bucket %d", bucketID)
				require.NotNil(t, debugs, "should return debugs")
				require.NoError(t, err, "should not return error")
			}

			// We waited
			executionTime = executionTime.Add(limit1.durationPerToken)

			// Buckets should be empty again
			for bucketID := range buckets {
				ok := limiters.allowN(bucketID, executionTime, 1)
				require.False(t, ok, "should not allow again immediately after wait for bucket %d", bucketID)
			}
		})

		t.Run("Concurrent", func(t *testing.T) {
			// t.Parallel()
			// GitHub Actions are pretty resource constrained,
			// and this test is sensitive to timing.
			keyFunc := func(input int64) string {
				return fmt.Sprintf("test-bucket-%d", input)
			}
			const buckets int64 = 3
			// Create two limiters with different limits
			limit1 := NewLimit(2, 100*time.Millisecond) // 2 per 100ms (more restrictive)
			limit2 := NewLimit(4, 100*time.Millisecond) // 4 per 100ms
			limiter1 := NewLimiter(keyFunc, limit1)
			limiter2 := NewLimiter(keyFunc, limit2)
			limiters := Combine(limiter1, limiter2)
			executionTime := ntime.Now()

			// Exhaust tokens for all buckets from the more restrictive limiter
			for bucketID := range buckets {
				for range limit1.count {
					allow := limiters.allowN(bucketID, executionTime, 1)
					require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
				}
			}

			// Buckets should be empty (limiter1 exhausted)
			for bucketID := range buckets {
				allow := limiters.allowN(bucketID, executionTime, 1)
				require.False(t, allow, "should not allow when limiter1 tokens exhausted for bucket %d", bucketID)
			}

			// Test 1: Multiple goroutines with immediate cancellation
			{
				concurrency := buckets
				results := make([]bool, concurrency)

				// Context that's immediately cancelled (closed)
				done := make(chan struct{})
				ctx := &testContext{
					done: done,
				}
				close(done)

				// Start concurrent waits
				var wg sync.WaitGroup
				for i := range concurrency {
					wg.Add(1)
					go func(i int64) {
						defer wg.Done()
						allow, _, err := limiters.waitNWithDebug(ctx, i, executionTime, 1)
						require.False(t, allow, "should not acquire token due to immediate cancellation")
						require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
						results[i] = allow
					}(i)
				}
				wg.Wait()

				// All should fail because context is cancelled immediately
				for i, result := range results {
					require.False(t, result, "goroutine %d should not acquire token due to immediate cancellation", i)
				}
			}

			// Test 2: Multiple goroutines with delayed cancellation
			{
				keyFunc := func(input int64) int64 {
					return input
				}

				limit1 := NewLimit(1, 200*time.Millisecond)
				limit2 := NewLimit(3, 200*time.Millisecond)
				limiter1 := NewLimiter(keyFunc, limit1)
				limiter2 := NewLimiter(keyFunc, limit2)
				limiters := Combine(limiter1, limiter2)

				// Exhaust tokens
				for bucketID := range buckets {
					// limit 1 being exhausted means the overall combined limiters are exhausted
					for range limit1.count {
						allow := limiters.allowN(bucketID, executionTime, 1)
						require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
					}
				}

				// Confirm exhausted
				for bucketID := range buckets {
					allow := limiters.allowN(bucketID, executionTime, 1)
					require.False(t, allow, "should not allow when limiter1 tokens exhausted for bucket %d", bucketID)
				}

				// We're trying to emulate a wait call
				// that starts waiting with a retry, but
				// is cancelled in the meantime.

				const buckets int64 = 3
				results := make([]bool, buckets)

				var wg sync.WaitGroup
				for bucketID := range buckets {
					wg.Add(1)

					done := make(chan struct{})
					ctx := &testContext{
						done: done,
					}

					go func(bucketID int64) {
						defer wg.Done()
						// Should start waiting, since the bucket is empty.
						// retryAfter should be ~200ms
						allow, _, err := limiters.waitNWithDebug(ctx, bucketID, executionTime, 1)
						require.False(t, allow, "should not acquire token due to cancellation, bucketID: %d", bucketID)
						require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
						results[bucketID] = allow
					}(bucketID)

					// Cancel context after goroutine has started.
					// Originally, this had a delay, for a better test,
					// but it is flaky on GitHub Actions, presumably
					// because the runner is resource constrained.
					// Delay works fine locally on Mac M2.
					close(done)
				}
				wg.Wait()

				// All should fail because context is cancelled
				for i, result := range results {
					require.False(t, result, "goroutine %d should not acquire token due to cancellation", i)
				}
			}
		})
	})
}
