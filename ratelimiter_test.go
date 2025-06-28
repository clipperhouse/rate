package ratelimiter

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRateLimiter_Allow_SingleBucket(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewRateLimiter(keyer, limit)

	now := time.Now()

	for range 9 {
		require.True(t, limiter.allow("test", now))
	}

	require.False(t, limiter.allow("test", now))

	// A token is ~111ms
	now = now.Add(time.Millisecond * 120)

	require.True(t, limiter.allow("test", now))
}

func TestRateLimiter_Allow_MultipleBuckets(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewRateLimiter(keyer, limit)
	now := time.Now()

	for i := range buckets {
		for range 9 {
			limiter.allow(fmt.Sprintf("test-%d", i), now)
		}
		require.False(t, limiter.allow(fmt.Sprintf("test-%d", i), now))
	}
}

func TestRateLimiter_Allow_MultipleBuckets_Concurrent(t *testing.T) {
	keyer := func(bucketID int) string {
		return fmt.Sprintf("test-bucket-%d", bucketID)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewRateLimiter(keyer, limit)
	start := time.Now()

	var wg sync.WaitGroup

	// Enough concurrent processes for each bucket to precisely exhaust the limit
	for bucketID := range buckets {
		for processID := range limit.Count {
			wg.Add(1)
			go func(bucketID int, processID int64) {
				defer wg.Done()
				allowed := limiter.allow(bucketID, start)
				require.True(t, allowed, "process %d for bucket %s should be allowed", processID, keyer(bucketID))
			}(bucketID, processID)
		}
	}

	wg.Wait()

	// Verify that additional requests are rejected, all buckets should be exhausted
	for bucketID := range buckets {
		allowed := limiter.allow(bucketID, start)
		require.False(t, allowed, "bucket %d should be exhausted after %d requests", bucketID, limit.Count)
	}

	// Complete refill
	now := start.Add(time.Second)
	for bucketID := range buckets {
		for range limit.Count {
			allowed := limiter.allow(bucketID, now)
			require.True(t, allowed, "bucket %d should be refilled after 1 second", bucketID)
		}
	}
}
