package ratelimiter_test

import (
	"sync"
	"testing"
	"time"

	"github.com/clipperhouse/ratelimiter"
	"github.com/stretchr/testify/require"
)

func TestBucket_RemainingTokens(t *testing.T) {
	now := time.Now()
	limit := ratelimiter.NewLimit(9, time.Second)
	bucket := ratelimiter.NewBucket(now, limit)

	actual := bucket.RemainingTokens(now, limit)
	expected := int64(9)
	require.Equal(t, expected, actual, "remaining tokens should equal to limit count")
}

func TestBucket_ConsumeToken(t *testing.T) {
	now := time.Now()
	limit := ratelimiter.NewLimit(9, time.Second)
	bucket := ratelimiter.NewBucket(now, limit)
	bucket.ConsumeToken(limit)

	actual := bucket.RemainingTokens(now, limit)
	expected := int64(8)
	require.Equal(t, expected, actual, "remaining tokens should be one less after consumption")
}

func TestBucket_Allow(t *testing.T) {
	limit := ratelimiter.NewLimit(9, time.Minute)
	bucket := ratelimiter.NewBucket(time.Now(), limit)

	for range 9 {
		actual := bucket.Allow(time.Now(), limit)
		require.True(t, actual, "expected to allow request when tokens are available")
	}

	// Should be gone now
	actual := bucket.Allow(time.Now(), limit)
	require.False(t, actual, "expected to deny request after tokens are exhausted")
}

func TestBucket_Concurrency(t *testing.T) {
	limit := ratelimiter.NewLimit(10, time.Second)
	bucket := ratelimiter.NewBucket(time.Now(), limit)

	var wg sync.WaitGroup
	concurrency := 100
	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()
			bucket.Allow(time.Now(), limit)
		}()
	}

	wg.Wait()

	// We expect that all tokens are consumed
	actual := bucket.RemainingTokens(time.Now(), limit)
	expected := int64(0)
	require.Equal(t, expected, actual, "Expected all tokens to be consumed, but some remained")
}
