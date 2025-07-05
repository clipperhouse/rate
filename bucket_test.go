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

	for i := range limit.count {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			actual := bucket.allow(now, limit)
			require.True(t, actual, "expected to allow request when tokens are available (goroutine %d)", i)
		}(i)
	}

	wg.Wait()

	// Tokens should be gone now
	actual := bucket.allow(now, limit)
	require.False(t, actual, "expected to deny request after tokens are exhausted")

	now = now.Add(limit.durationPerToken)

	// A new token should have refilled by now
	actual = bucket.allow(now, limit)
	require.True(t, actual, "expected to allow request after waiting for refill")
}

func TestBucket_Peek(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	for range limit.count * 2 {
		// any number of peeks should return true
		actual := bucket.peek(now, limit)
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
		actual := bucket.peek(now, limit)
		require.False(t, actual, "expected to deny peek requests when all tokens are gone")
	}

	// Refill one token
	now = now.Add(limit.durationPerToken)
	require.True(t, bucket.peek(now, limit), "peek should return true after tokens refill")
}

func TestBucket_Peek_Concurrent(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	// any number of peeks should return true
	{
		var wg sync.WaitGroup

		for i := range limit.count * 2 {
			wg.Add(1)
			go func(index int64) {
				defer wg.Done()
				actual := bucket.peek(now, limit)
				require.True(t, actual, "expected any number of peeks to be true (goroutine %d)", index)
			}(i)
		}

		wg.Wait()
	}

	// interleave allows and peeks
	{
		var wg sync.WaitGroup

		for i := range limit.count {
			wg.Add(1)
			go func(index int64) {
				defer wg.Done()
				peek := bucket.peek(now, limit)
				require.True(t, peek, "expected peek to be true (goroutine %d)", index)
				allow := bucket.allow(now, limit)
				require.True(t, allow, "expected allow to be true (goroutine %d)", index)
			}(i)
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

	var wg sync.WaitGroup
	ctx := context.Background()

	// These waits should queue up and execute as tokens become available
	for i := range limit.count {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			allow := bucket.wait(ctx, now, limit)
			require.True(t, allow, "should acquire token after waiting (goroutine %d)", i)
		}(i)
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
		ok := bucket.allow(executionTime, limit)
		require.False(t, ok, "should not allow when tokens exhausted")
	}

	// Test 1: Wait with enough time to acquire a token
	{
		// Mock deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken), true
		}

		// Mock done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
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
			return make(chan struct{}) // never closes
		}

		allow := bucket.waitWithCancellation(executionTime, limit, deadline, done)
		require.False(t, allow, "should not acquire token if deadline expires before token is available")
	}

	// Test 3: Wait with immediate cancellation
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken + 10*time.Millisecond), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
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
			return make(chan struct{}) // never closes
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
		ok := bucket.allow(executionTime, limit)
		require.True(t, ok, "should allow initial tokens")
	}

	// All tokens should be exhausted
	require.False(t, bucket.allow(executionTime, limit), "should not allow when tokens exhausted")

	// Test 1: Multiple goroutines competing for tokens with enough time
	{
		// More goroutines than available tokens
		concurrency := limit.count * 3
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
				results[i] = bucket.waitWithCancellation(executionTime, limit, deadline, done)
			}(i)
		}

		wg.Wait()

		// We waited
		executionTime = executionTime.Add(limit.period)

		// Count successes and failures
		var successCount, failureCount int64
		for _, result := range results {
			if result {
				successCount++
			} else {
				failureCount++
			}
		}

		// Exactly limit.count goroutines should have acquired a token
		require.Equal(t, limit.count, successCount, "expected exactly %d goroutines to acquire a token", limit.count)
		// The rest should have been denied
		require.Equal(t, concurrency-limit.count, failureCount, "expected %d goroutines to fail", concurrency-limit.count)

		// All tokens should be exhausted again
		allow := bucket.allow(executionTime, limit)
		require.False(t, allow, "should not allow after all waits have completed")
	}

	// Test 2: Multiple goroutines with deadline that expires before tokens are available
	{
		// More goroutines than available tokens
		concurrency := limit.count * 3
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
				results[i] = bucket.waitWithCancellation(executionTime, limit, deadline, done)
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
		concurrency := limit.count
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
				results[i] = bucket.waitWithCancellation(executionTime, limit, deadline, done)
			}(i)
		}

		wg.Wait()

		// All should fail because context is cancelled immediately
		for i, result := range results {
			require.False(t, result, "goroutine %d should not acquire token due to immediate cancellation", i)
		}
	}

	// Test 4: Multiple goroutines with staggered deadlines
	{
		concurrency := limit.count * 3
		results := make([]bool, concurrency)
		var wg sync.WaitGroup

		// Start concurrent waits with different deadlines
		for i := range concurrency {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()

				// Each goroutine gets a different deadline
				deadlineOffset := time.Duration(i) * limit.durationPerToken
				deadline := func() (time.Time, bool) {
					return executionTime.Add(deadlineOffset + limit.durationPerToken + 5*time.Millisecond), true
				}

				done := func() <-chan struct{} {
					return make(chan struct{}) // never closes
				}

				results[i] = bucket.waitWithCancellation(executionTime, limit, deadline, done)
			}(i)
		}

		wg.Wait()

		// Count successes
		var successCount int64
		for _, result := range results {
			if result {
				successCount++
			}
		}

		// Some should succeed (those with deadlines after tokens become available)
		// and some should fail (those with deadlines before tokens become available)
		require.Greater(t, successCount, int64(0), "expected some goroutines to succeed")
		require.Less(t, successCount, concurrency, "expected some goroutines to fail")
	}
}
