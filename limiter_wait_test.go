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

func TestLimiter_Wait(t *testing.T) {
	t.Parallel()

	t.Run("SingleBucket", func(t *testing.T) {
		t.Parallel()
		keyFunc := func(input string) string {
			return input
		}
		limit := NewLimit(2, 100*time.Millisecond)
		limiter := NewLimiter(keyFunc, limit)

		executionTime := ntime.Now()

		// Consume all tokens
		for range limit.count {
			ok := limiter.allow("test", executionTime)
			require.True(t, ok, "should allow initial tokens")
		}

		// Should not allow immediately
		ok := limiter.allow("test", executionTime)
		require.False(t, ok, "should not allow when tokens exhausted")

		// Test 1: Wait for 1 token with no cancellation
		{
			ctx := &testContext{
				done: make(chan struct{}), // never closes
			}

			allow, details, err := limiter.waitNWithDetails("test", executionTime, 1, ctx)
			require.NoError(t, err, "should not return error")
			require.True(t, allow, "should acquire 1 token after waiting")
			require.NotNil(t, details, "should return details")

			// We waited
			executionTime = executionTime.Add(limit.durationPerToken)
		}

		// Should not allow again immediately after wait
		ok = limiter.allow("test", executionTime)
		require.False(t, ok, "should not allow again immediately after wait")

		// Test 2: Wait for 1 token with immediate cancellation
		{
			// Context that's immediately cancelled (closed)
			done := make(chan struct{})
			ctx := &testContext{
				done: done,
			}
			close(done)

			allow, details, err := limiter.waitNWithDetails("test", executionTime, 1, ctx)
			require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
			require.False(t, allow, "should not acquire 1 token if context is cancelled immediately")
			require.NotNil(t, details, "should return details even on cancellation")
		}

		// Test 3: Wait for multiple tokens (limit.count) with no cancellation
		{
			// Wait for bucket to refill enough tokens
			executionTime = executionTime.Add(limit.durationPerToken * time.Duration(limit.count))

			ctx := &testContext{
				done: make(chan struct{}), // never closes
			}

			allow, details, err := limiter.waitNWithDetails("test", executionTime, limit.count, ctx)
			require.NoError(t, err, "should not return error")
			require.True(t, allow, "should acquire %d tokens after waiting", limit.count)
			require.NotNil(t, details, "should return details")

			// We waited
			executionTime = executionTime.Add(limit.durationPerToken)
		}

		// Test 4: Wait for multiple tokens with immediate cancellation
		{
			// Wait for bucket to refill enough tokens
			executionTime = executionTime.Add(limit.durationPerToken * time.Duration(limit.count))

			// Consume all tokens to exhaust the bucket again
			for range limit.count {
				ok := limiter.allow("test", executionTime)
				require.True(t, ok, "should allow tokens to exhaust bucket")
			}

			// Context that's immediately cancelled (closed)
			done := make(chan struct{})
			ctx := &testContext{
				done: done,
			}
			close(done)

			allow, details, err := limiter.waitNWithDetails("test", executionTime, limit.count, ctx)
			require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
			require.False(t, allow, "should not acquire %d tokens if context is cancelled immediately", limit.count)
			require.NotNil(t, details, "should return details even on cancellation")
		}
	})

	t.Run("MultipleBuckets", func(t *testing.T) {
		t.Run("Serial", func(t *testing.T) {
			t.Parallel()
			keyFunc := func(input int) string {
				return fmt.Sprintf("test-bucket-%d", input)
			}
			const buckets = 3
			limit := NewLimit(2, 100*time.Millisecond)
			limiter := NewLimiter(keyFunc, limit)
			executionTime := ntime.Now()

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
				ctx := &testContext{
					done: make(chan struct{}), // never closes
				}

				allow, details, err := limiter.waitNWithDetails(bucketID, executionTime, 1, ctx)
				require.NoError(t, err, "should not return error")
				require.True(t, allow, "should acquire token after waiting for bucket %d", bucketID)
				require.NotNil(t, details, "should return details")
			}

			// We waited
			executionTime = executionTime.Add(limit.durationPerToken)

			// Buckets should be empty again
			for bucketID := range buckets {
				ok := limiter.allow(bucketID, executionTime)
				require.False(t, ok, "should not allow again immediately after wait for bucket %d", bucketID)
			}
		})

		t.Run("Concurrent", func(t *testing.T) {
			t.Parallel()
			keyFunc := func(input int64) string {
				return fmt.Sprintf("test-bucket-%d", input)
			}
			const buckets int64 = 3
			limit := NewLimit(2, 100*time.Millisecond)
			limiter := NewLimiter(keyFunc, limit)
			executionTime := ntime.Now()

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
						allow, _, err := limiter.waitNWithDetails(i, executionTime, 1, ctx)
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
				concurrency := buckets
				results := make([]bool, concurrency)

				// Context that gets cancelled after a short delay
				done := make(chan struct{})
				ctx := &testContext{
					done: done,
				}

				// Start concurrent waits
				var wg sync.WaitGroup
				for i := range concurrency {
					wg.Add(1)
					go func(i int64) {
						defer wg.Done()
						allow, _, err := limiter.waitNWithDetails(i, executionTime, 1, ctx)
						require.ErrorIs(t, err, context.Canceled, "should return context canceled error")
						results[i] = allow
					}(i)
				}

				// Cancel context after a short delay
				go func() {
					time.Sleep(2 * time.Millisecond)
					close(done)
				}()

				wg.Wait()

				// All should fail because context is cancelled
				for i, result := range results {
					require.False(t, result, "goroutine %d should not acquire token due to cancellation", i)
				}
			}
		})
	})
}

var _ context.Context = &testContext{}

type testContext struct {
	done <-chan struct{}
}

func (t *testContext) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

func (t *testContext) Done() <-chan struct{} {
	return t.done
}

func (t *testContext) Err() error {
	select {
	case <-t.done:
		return context.Canceled
	default:
		return nil
	}
}

func (t *testContext) Value(key any) any {
	return nil
}
