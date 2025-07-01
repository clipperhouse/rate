package rate

import (
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
		expected := limit.Count
		require.Equal(t, expected, actual, "remaining tokens should equal to limit count")
	}

	now = now.Add(time.Hour)
	{
		actual := bucket.remainingTokens(now, limit)
		expected := limit.Count
		require.Equal(t, expected, actual, "remaining tokens should equal to limit count after a long time")
	}
}

func TestBucket_ConsumeToken(t *testing.T) {
	now := time.Now()
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	for i := range limit.Count {
		bucket.consumeToken(limit)
		actual := bucket.remainingTokens(now, limit)
		expected := limit.Count - i - 1
		require.Equal(t, expected, actual, "remaining tokens should be one less after consumption")
	}
}

func TestBucket_Allow(t *testing.T) {
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	for range limit.Count {
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
		now = now.Add(time.Millisecond * 112)
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
	for range limit.Count {
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

	for i := range limit.Count {
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

	now = now.Add(time.Millisecond * 120)

	// A new token should have refilled by now
	actual = bucket.allow(now, limit)
	require.True(t, actual, "expected to allow request after waiting for refill")
}

func TestBucket_Peek(t *testing.T) {
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	for range limit.Count * 2 {
		// any number of peeks should return true
		actual := bucket.peek(now, limit)
		require.True(t, actual, "expected to allow request with any number of peeks")
	}

	// Consume all the tokens
	for range limit.Count {
		actual := bucket.allow(now, limit)
		require.True(t, actual, "expected to allow requests as normal when peek previously called")
	}

	require.False(t, bucket.allow(now, limit), "should have exhausted tokens")

	for range limit.Count * 2 {
		// any number of peeks should return false with no remianing tokens
		actual := bucket.peek(now, limit)
		require.False(t, actual, "expected to deny peek requests when all tokens are gone")
	}

	// Refill one token
	now = now.Add(time.Millisecond * 112)
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

		for i := range limit.Count * 2 {
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

		for i := range limit.Count {
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
