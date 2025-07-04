package rate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBucket_RemainingTokens(t *testing.T) {
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

func TestBucket_Wait(t *testing.T) {
	now := time.Now()
	limit := NewLimit(2, 100*time.Millisecond) // 2 tokens per 100ms (1 every 50ms)
	bucket := newBucket(now, limit)

	// Consume all tokens
	for range limit.count {
		ok := bucket.allow(now, limit)
		require.True(t, ok, "should allow initial tokens")
	}

	// Should not allow immediately
	{
		ok := bucket.allow(now, limit)
		require.False(t, ok, "should not allow when tokens exhausted")
	}

	// Wait for a token with enough context timeout
	{
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(limit.durationPerToken, cancel)
		allow := bucket.wait(ctx, now, limit)
		require.True(t, allow, "should acquire token after waiting")

		// we waited
		now = now.Add(limit.durationPerToken)

		// Should not allow again immediately
		ok := bucket.allow(now, limit)
		require.False(t, ok, "should not allow again immediately after wait")
	}

	// Wait with a context without enough time to acquire a token
	{
		ctx, cancel := context.WithCancel(context.Background())
		fudge := 2 * time.Millisecond // account for timing imprecision, ugh
		time.AfterFunc(limit.durationPerToken-fudge, cancel)
		allow := bucket.wait(ctx, now, limit)
		require.False(t, allow, "should not acquire token if context is cancelled before next token is available")
	}

	// Wait again for a token with enough context timeout
	{
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(limit.durationPerToken, cancel)
		allow := bucket.wait(ctx, now, limit)
		require.True(t, allow, "should acquire token after waiting again")

		// we waited
		now = now.Add(limit.durationPerToken)

		// Should not allow again immediately
		ok := bucket.allow(now, limit)
		require.False(t, ok, "should not allow again immediately after wait")
	}
}

func TestBucket_Wait_Concurrent(t *testing.T) {
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

func TestBucket_Wait_Concurrent_With_Timeout(t *testing.T) {
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
	// The context will be cancelled after enough time for all tokens to be refilled
	ctx, cancel := context.WithCancel(context.Background())

	const fudge = 4 * time.Millisecond // account for timing imprecision, ugh
	time.AfterFunc(limit.period+fudge, cancel)

	concurrency := limit.count * 3 // Start more goroutines than available tokens
	successes := make(chan bool, concurrency)

	// These waits should queue up and execute as tokens become available.
	for i := range concurrency {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			allow := bucket.wait(ctx, now, limit)
			successes <- allow
		}(i)
	}

	wg.Wait()
	close(successes)

	var successCount, failureCount int64
	for s := range successes {
		if s {
			successCount++
		} else {
			failureCount++
		}
	}

	// Exactly limit.count goroutines should have acquired a token
	require.Equal(t, limit.count, successCount, "expected some goroutines to acquire a token")
	// The rest should have been cancelled
	require.Equal(t, concurrency-limit.count, failureCount, "expected some goroutines to be cancelled")

	// All tokens should be exhausted again
	require.False(t, bucket.allow(now.Add(limit.period), limit), "should not allow after all waits have completed")
}
