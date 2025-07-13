package rate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBucket_RemainingTokens(t *testing.T) {
	t.Parallel()
	now := time.Now()
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	{
		actual := bucket.remainingTokens(now, limit)
		expected := limit.count
		require.Equal(t, expected, actual, "remaining tokens should equal to limit count")
	}

	now = now.Add(time.Hour)
	{
		actual := bucket.remainingTokens(now, limit)
		expected := limit.count
		require.Equal(t, expected, actual, "remaining tokens should equal to limit count after a long time")
	}
}

func TestBucket_RemainingTokens_Concurrent(t *testing.T) {
	t.Parallel()
	now := time.Now()
	limit := NewLimit(100, time.Second)
	bucket := newBucket(now, limit)

	// Number of concurrent operations
	const numOps = 200

	var wg sync.WaitGroup
	for i := range numOps {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Interleave consumption and reading
			if i%2 == 0 {
				bucket.ConsumeToken(limit)
				return
			}

			// We can't assert a specific value, but we can ensure it's within bounds
			// and doesn't crash.
			remaining := bucket.RemainingTokens(now, limit)
			require.GreaterOrEqual(t, remaining, int64(0), "remaining tokens should not be negative")
			require.LessOrEqual(t, remaining, limit.count, "remaining tokens should not exceed limit")

		}(i)
	}
	wg.Wait()

	// 100 tokens should have been consumed after all
	expected := limit.count - (numOps / 2)
	actual := bucket.RemainingTokens(now, limit)
	require.Equal(t, expected, actual, "final token count should be correct")
}

func TestBucket_ConsumeToken(t *testing.T) {
	t.Parallel()
	now := time.Now()
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	for i := range limit.count {
		bucket.consumeToken(limit)
		actual := bucket.remainingTokens(now, limit)
		expected := limit.count - i - 1
		require.Equal(t, expected, actual, "remaining tokens should be one less after consumption")
	}
}

func TestBucket_Allow(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	for range limit.count {
		actual := bucket.allow(now, limit)
		require.True(t, actual, "expected to allow request when tokens are available")
	}

	// Tokens should be gone now
	{
		actual := bucket.allow(now, limit)
		require.False(t, actual, "expected to deny request after tokens are exhausted")
	}

	// Refill with one token and consume it
	{
		now = now.Add(limit.durationPerToken)
		actual := bucket.allow(now, limit)
		require.True(t, actual, "expected to allow request after waiting for refill")
	}

	// Token should be gone now
	{
		actual := bucket.allow(now, limit)
		require.False(t, actual, "expected to deny request after one refilled token is consumed")
	}

	// If the bucket is old, it should not mistakenly be interpreted as having too many tokens
	now = now.Add(time.Hour)
	for range limit.count {
		actual := bucket.allow(now, limit)
		require.True(t, actual, "expected to allow requests with old bucket")
	}

	// Tokens should be gone now
	{
		actual := bucket.allow(now, limit)
		require.False(t, actual, "expected to deny request after old bucket's tokens are exhausted")
	}
}

func TestBucket_Allow_Concurrent(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	var wg sync.WaitGroup
	for processID := range limit.count {
		wg.Add(1)
		go func(processID int64) {
			defer wg.Done()
			actual := bucket.Allow(now, limit)
			require.True(t, actual, "expected to allow request when tokens are available (goroutine %d)", processID)
		}(processID)
	}
	wg.Wait()

	// Tokens should be gone now
	actual := bucket.Allow(now, limit)
	require.False(t, actual, "expected to deny request after tokens are exhausted")

	now = now.Add(limit.durationPerToken)

	// A new token should have refilled by now
	actual = bucket.Allow(now, limit)
	require.True(t, actual, "expected to allow request after waiting for refill")
}

func TestBucket_HasToken(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	for range limit.count * 2 {
		// any number of peeks should return true
		actual := bucket.hasToken(now, limit)
		require.True(t, actual, "expected to allow request with any number of peeks")
	}

	// Consume all the tokens
	for range limit.count {
		actual := bucket.allow(now, limit)
		require.True(t, actual, "expected to allow requests as normal when peek previously called")
	}

	require.False(t, bucket.allow(now, limit), "should have exhausted tokens")

	for range limit.count * 2 {
		// any number of peeks should return false with no remianing tokens
		actual := bucket.hasToken(now, limit)
		require.False(t, actual, "expected to deny peek requests when all tokens are gone")
	}

	// Refill one token
	now = now.Add(limit.durationPerToken)
	require.True(t, bucket.hasToken(now, limit), "peek should return true after tokens refill")
}

func TestBucket_HasToken_Concurrent(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	// any number of peeks should return true
	{
		var wg sync.WaitGroup
		for processID := range limit.count * 2 {
			wg.Add(1)
			go func(processID int64) {
				defer wg.Done()
				actual := bucket.HasToken(now, limit)
				require.True(t, actual, "expected any number of peeks to be true (goroutine %d)", processID)
			}(processID)
		}
		wg.Wait()
	}

	// interleave allows and peeks
	{
		var wg sync.WaitGroup
		for processID := range limit.count {
			wg.Add(1)
			go func(processID int64) {
				defer wg.Done()
				peek := bucket.HasToken(now, limit)
				require.True(t, peek, "expected peek to be true (goroutine %d)", processID)
				allow := bucket.Allow(now, limit)
				require.True(t, allow, "expected allow to be true (goroutine %d)", processID)
			}(processID)
		}
		wg.Wait()
	}
}

func TestBucket_Wait_Concurrent(t *testing.T) {
	t.Parallel()
	now := time.Now()
	limit := NewLimit(5, 100*time.Millisecond)
	bucket := newBucket(now, limit)

	// Consume all initial tokens
	for range limit.count {
		ok := bucket.allow(now, limit)
		require.True(t, ok, "should allow initial tokens")
	}

	// All tokens should be exhausted
	require.False(t, bucket.allow(now, limit), "should not allow when tokens exhausted")

	ctx := context.Background()

	// These waits should queue up and execute as tokens become available
	var wg sync.WaitGroup
	for processID := range limit.count {
		wg.Add(1)
		go func(processID int64) {
			defer wg.Done()
			allow := bucket.wait(ctx, now, limit)
			require.True(t, allow, "should acquire token after waiting (goroutine %d)", processID)
		}(processID)
	}
	wg.Wait()

	// All tokens should be exhausted again
	require.False(t, bucket.allow(now.Add(limit.period), limit), "should not allow after all waits have completed")
}

func TestBucket_WaitWithCancellation(t *testing.T) {
	t.Parallel()
	executionTime := time.Now()
	limit := NewLimit(2, 100*time.Millisecond)
	bucket := newBucket(executionTime, limit)

	// Consume all tokens
	for range limit.count {
		ok := bucket.allow(executionTime, limit)
		require.True(t, ok, "should allow initial tokens")
	}

	// Should not allow immediately
	{
		allow := bucket.allow(executionTime, limit)
		require.False(t, allow, "should not allow when tokens exhausted")
	}

	// Test 1: Wait with enough time to acquire a token
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken), true
		}

		// Done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{})
		}

		allow := bucket.waitWithCancellation(executionTime, limit, deadline, done)
		require.True(t, allow, "should acquire token after waiting")

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)

		// Should not allow again immediately
		ok := bucket.allow(executionTime, limit)
		require.False(t, ok, "should not allow again immediately after wait")
	}

	// Test 2: Wait with deadline that expires before token is available
	{
		// Deadline that expires too soon
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken / 2), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{})
		}

		allow := bucket.waitWithCancellation(executionTime, limit, deadline, done)
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
			close(ch)
			return ch
		}

		allow := bucket.waitWithCancellation(executionTime, limit, deadline, done)
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
			return make(chan struct{})
		}

		{
			allow := bucket.waitWithCancellation(executionTime, limit, deadline, done)
			require.True(t, allow, "should acquire token when no deadline is set")
		}

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)

		// Should not allow again immediately
		{
			allow := bucket.allow(executionTime, limit)
			require.False(t, allow, "should not allow again immediately after wait")
		}
	}
}

func TestBucket_WaitWithCancellation_Concurrent(t *testing.T) {
	t.Parallel()
	executionTime := time.Now()
	limit := NewLimit(5, 100*time.Millisecond) // 5 tokens per 100ms (1 every 20ms)
	bucket := newBucket(executionTime, limit)

	// Consume all initial tokens
	for range limit.count {
		allow := bucket.allow(executionTime, limit)
		require.True(t, allow, "should allow initial tokens")
	}

	// All tokens should be exhausted
	require.False(t, bucket.allow(executionTime, limit), "should not allow when tokens exhausted")

	// Test 1: Multiple goroutines competing for tokens with enough time
	{
		// More goroutines than available tokens
		concurrency := limit.count * 3
		results := make([]bool, concurrency)

		// Deadline that gives enough time for all tokens to be refilled
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.period), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{})
		}

		var wg sync.WaitGroup
		for processID := range concurrency {
			wg.Add(1)
			go func(processID int64) {
				defer wg.Done()
				results[processID] = bucket.waitWithCancellation(executionTime, limit, deadline, done)
			}(processID)
		}
		wg.Wait()

		// We waited
		executionTime = executionTime.Add(limit.period)

		// Count successes and failures
		var sucesses, failures int64
		for _, result := range results {
			if result {
				sucesses++
			} else {
				failures++
			}
		}

		// Exactly limit.count goroutines should have acquired a token
		require.Equal(t, limit.count, sucesses, "expected exactly %d goroutines to acquire a token", limit.count)
		// The rest should have been denied
		require.Equal(t, concurrency-limit.count, failures, "expected %d goroutines to fail", concurrency-limit.count)

		// All tokens should be exhausted again
		allow := bucket.allow(executionTime, limit)
		require.False(t, allow, "should not allow after all waits have completed")
	}

	// Test 2: Multiple goroutines with deadline that expires before tokens are available
	{
		// More goroutines than available tokens
		concurrency := limit.count * 3
		results := make([]bool, concurrency)

		// Deadline that expires too soon
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken / 2), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{})
		}

		var wg sync.WaitGroup
		for processID := range concurrency {
			wg.Add(1)
			go func(processID int64) {
				defer wg.Done()
				results[processID] = bucket.waitWithCancellation(executionTime, limit, deadline, done)
			}(processID)
		}
		wg.Wait()

		// All should fail because deadline expires before tokens are available
		for processID, result := range results {
			require.False(t, result, "goroutine %d should not acquire token due to early deadline", processID)
		}
	}

	// Test 3: Multiple goroutines with immediate cancellation
	{
		concurrency := limit.count
		results := make([]bool, concurrency)

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.period), true
		}

		// Done channel that closes (cancels) immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch)
			return ch
		}

		// Start concurrent waits
		var wg sync.WaitGroup
		for processID := range concurrency {
			wg.Add(1)
			go func(processID int64) {
				defer wg.Done()
				results[processID] = bucket.waitWithCancellation(executionTime, limit, deadline, done)
			}(processID)
		}
		wg.Wait()

		// All should fail because context is cancelled immediately
		for processID, result := range results {
			require.False(t, result, "goroutine %d should not acquire token due to immediate cancellation", processID)
		}
	}

	// Test 4: Multiple goroutines with staggered deadlines
	{
		concurrency := limit.count * 3
		results := make([]bool, concurrency)

		var wg sync.WaitGroup
		for processID := range concurrency {
			wg.Add(1)
			go func(processID int64) {
				defer wg.Done()

				// Each goroutine gets a different deadline
				deadlineOffset := time.Duration(processID) * limit.durationPerToken
				deadline := func() (time.Time, bool) {
					return executionTime.Add(deadlineOffset + limit.durationPerToken), true
				}

				// never cancels
				done := func() <-chan struct{} {
					return make(chan struct{})
				}

				results[processID] = bucket.waitWithCancellation(executionTime, limit, deadline, done)
			}(processID)
		}

		wg.Wait()

		// Count successes
		var successes int64
		for _, result := range results {
			if result {
				successes++
			}
		}

		// Some should succeed (those with deadlines after tokens become available)
		// and some should fail (those with deadlines before tokens become available)
		require.Greater(t, successes, int64(0), "expected some goroutines to succeed")
		require.Less(t, successes, concurrency, "expected some goroutines to fail")
	}
}
