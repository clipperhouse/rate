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

		// Test 1: Wait for 1 token with enough time to acquire it
		{
			// Context with deadline that gives enough time
			ctx := &testContext{
				deadline:    executionTime.Add(limit.durationPerToken).ToTime(),
				hasDeadline: true,
				done:        make(chan struct{}), // never closes
			}

			allow, details := limiter.waitNWithDetails("test", executionTime, 1, ctx)
			require.True(t, allow, "should acquire 1 token after waiting")
			require.NotNil(t, details, "should return details")

			// We waited
			executionTime = executionTime.Add(limit.durationPerToken)
		}

		// Should not allow again immediately after wait
		ok = limiter.allow("test", executionTime)
		require.False(t, ok, "should not allow again immediately after wait")

		// Test 2: Wait for 1 token with deadline that expires before token is available
		{
			// Context with deadline that expires too soon
			ctx := &testContext{
				deadline:    executionTime.Add(limit.durationPerToken / 2).ToTime(),
				hasDeadline: true,
				done:        make(chan struct{}), // never closes
			}

			allow, details := limiter.waitNWithDetails("test", executionTime, 1, ctx)
			require.False(t, allow, "should not acquire 1 token if deadline expires before token is available")
			require.NotNil(t, details, "should return details even on deadline expiry")
		}

		// Test 3: Wait for 1 token with immediate cancellation
		{
			// Context that's immediately cancelled (closed)
			done := make(chan struct{})
			close(done)
			ctx := &testContext{
				deadline:    executionTime.Add(limit.durationPerToken).ToTime(),
				hasDeadline: true,
				done:        done,
			}

			allow, details := limiter.waitNWithDetails("test", executionTime, 1, ctx)
			require.False(t, allow, "should not acquire 1 token if context is cancelled immediately")
			require.NotNil(t, details, "should return details even on cancellation")
		}

		// Test 4: Wait for 1 token with no deadline
		{
			// Context with no deadline
			ctx := &testContext{
				deadline:    time.Time{},
				hasDeadline: false,
				done:        make(chan struct{}), // never closes
			}

			allow, details := limiter.waitNWithDetails("test", executionTime, 1, ctx)
			require.True(t, allow, "should acquire 1 token when no deadline is set")
			require.NotNil(t, details, "should return details")

			// We waited
			executionTime = executionTime.Add(limit.durationPerToken)

			// Should not allow again immediately
			ok := limiter.allow("test", executionTime)
			require.False(t, ok, "should not allow again immediately after wait")
		}

		// Test 5: Wait for multiple tokens (limit.count) with enough time to acquire them
		{
			// Wait for bucket to refill enough tokens
			executionTime = executionTime.Add(limit.durationPerToken * time.Duration(limit.count))

			wait := time.Duration(limit.count) * limit.durationPerToken
			// Context with deadline that gives enough time
			ctx := &testContext{
				deadline:    executionTime.Add(wait).ToTime(),
				hasDeadline: true,
				done:        make(chan struct{}), // never closes
			}

			allow, details := limiter.waitNWithDetails("test", executionTime, limit.count, ctx)
			require.True(t, allow, "should acquire %d tokens after waiting", limit.count)
			require.NotNil(t, details, "should return details")

			// We waited
			executionTime = executionTime.Add(limit.durationPerToken)
		}

		// Test 6: Wait for multiple tokens with deadline that expires before all tokens are available
		{
			// Wait for bucket to refill enough tokens
			executionTime = executionTime.Add(limit.durationPerToken * time.Duration(limit.count))

			// Consume all tokens to exhaust the bucket again
			for range limit.count {
				ok := limiter.allow("test", executionTime)
				require.True(t, ok, "should allow tokens to exhaust bucket")
			}

			// Context with deadline that expires too soon
			ctx := &testContext{
				deadline:    executionTime.Add(limit.durationPerToken / 4).ToTime(),
				hasDeadline: true,
				done:        make(chan struct{}), // never closes
			}

			allow, details := limiter.waitNWithDetails("test", executionTime, limit.count, ctx)
			require.False(t, allow, "should not acquire %d tokens if deadline expires before tokens are available", limit.count)
			require.NotNil(t, details, "should return details even on deadline expiry")
		}

		// Test 7: Wait for multiple tokens with immediate cancellation
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
			close(done)
			ctx := &testContext{
				deadline:    executionTime.Add(limit.durationPerToken * time.Duration(limit.count)).ToTime(),
				hasDeadline: true,
				done:        done,
			}

			allow, details := limiter.waitNWithDetails("test", executionTime, limit.count, ctx)
			require.False(t, allow, "should not acquire %d tokens if context is cancelled immediately", limit.count)
			require.NotNil(t, details, "should return details even on cancellation")
		}

		// Test 8: Wait for multiple tokens with no deadline
		{
			// Wait for bucket to refill enough tokens
			executionTime = executionTime.Add(limit.durationPerToken * time.Duration(limit.count))

			// Consume all tokens to exhaust the bucket again
			for range limit.count {
				ok := limiter.allow("test", executionTime)
				require.True(t, ok, "should allow tokens to exhaust bucket")
			}

			// Context with no deadline
			ctx := &testContext{
				deadline:    time.Time{},
				hasDeadline: false,
				done:        make(chan struct{}), // never closes
			}

			allow, details := limiter.waitNWithDetails("test", executionTime, limit.count, ctx)
			require.True(t, allow, "should acquire %d tokens when no deadline is set", limit.count)
			require.NotNil(t, details, "should return details")

			// We waited
			executionTime = executionTime.Add(limit.durationPerToken)

			// Should not allow again immediately
			allow = limiter.allowN("test", executionTime, limit.count)
			require.False(t, allow, "should not allow %d tokens again immediately after wait", limit.count)
		}
	})

	t.Run("MultipleBuckets", func(t *testing.T) {
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
			// Context with deadline that gives enough time
			ctx := &testContext{
				deadline:    executionTime.Add(limit.durationPerToken).ToTime(),
				hasDeadline: true,
				done:        make(chan struct{}), // never closes
			}

			allow, details := limiter.waitNWithDetails(bucketID, executionTime, 1, ctx)
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

	t.Run("MultipleBuckets_Concurrent", func(t *testing.T) {
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

		// Test 1: Multiple goroutines competing for tokens with enough time
		{
			// More goroutines than available tokens to create competition
			tokens := buckets * limit.count
			concurrency := tokens * 3 // oversubscribe by 3x
			results := make([]bool, concurrency)

			// Context with deadline that gives enough time for all tokens to be refilled
			ctx := &testContext{
				deadline:    executionTime.Add(limit.period).ToTime(),
				hasDeadline: true,
				done:        make(chan struct{}), // never closes
			}

			// Start concurrent waits
			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(i int64) {
					defer wg.Done()
					bucketID := i % buckets
					results[i], _ = limiter.waitNWithDetails(bucketID, executionTime, 1, ctx)
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

			// Context with deadline that expires too soon
			ctx := &testContext{
				deadline:    executionTime.Add(limit.durationPerToken / 2).ToTime(),
				hasDeadline: true,
				done:        make(chan struct{}), // never closes
			}

			// Start concurrent waits
			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(i int64) {
					defer wg.Done()
					results[i], _ = limiter.waitNWithDetails(i, executionTime, 1, ctx)
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

			// Context that's immediately cancelled (closed)
			done := make(chan struct{})
			close(done)
			ctx := &testContext{
				deadline:    executionTime.Add(limit.period).ToTime(),
				hasDeadline: true,
				done:        done,
			}

			// Start concurrent waits
			var wg sync.WaitGroup
			for i := range concurrency {
				wg.Add(1)
				go func(i int64) {
					defer wg.Done()
					results[i], _ = limiter.waitNWithDetails(i, executionTime, 1, ctx)
				}(i)
			}
			wg.Wait()

			// All should fail because context is cancelled immediately
			for i, result := range results {
				require.False(t, result, "goroutine %d should not acquire token due to immediate cancellation", i)
			}
		}
	})
}

var _ context.Context = &testContext{}

type testContext struct {
	deadline    time.Time
	hasDeadline bool
	done        <-chan struct{}
}

func (t *testContext) Deadline() (deadline time.Time, ok bool) {
	return t.deadline, t.hasDeadline
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
