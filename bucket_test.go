package ratelimiter

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
		now = now.Add(time.Millisecond)
	}

	// Tokens should be gone now
	actual := bucket.allow(now, limit)
	require.False(t, actual, "expected to deny request after tokens are exhausted")

	now = now.Add(time.Millisecond * 112)

	// One new token should have refilled by now
	actual = bucket.allow(now, limit)
	require.True(t, actual, "expected to allow request after waiting for refill")
}

func TestBucket_Allow_Concurrent(t *testing.T) {
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	var wg sync.WaitGroup

	for i := range 9 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			actual := bucket.allow(now, limit)
			require.True(t, actual, "expected to allow request when tokens are available (goroutine %d)", index)
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
