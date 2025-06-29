package rate

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiter_Allow_SingleBucket(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()

	for range 9 {
		require.True(t, limiter.allow("test", now))
	}

	require.False(t, limiter.allow("test", now))

	// A token is ~111ms
	now = now.Add(time.Millisecond * 120)

	require.True(t, limiter.allow("test", now))
}

func TestLimiter_Allow_MultipleBuckets(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
	now := time.Now()

	for i := range buckets {
		for range 9 {
			limiter.allow(fmt.Sprintf("test-%d", i), now)
		}
		require.False(t, limiter.allow(fmt.Sprintf("test-%d", i), now))
	}
}

func TestLimiter_Allow_MultipleBuckets_Concurrent(t *testing.T) {
	keyer := func(bucketID int) string {
		return fmt.Sprintf("test-bucket-%d", bucketID)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
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

func TestLimiter_Allow_MultipleLimits(t *testing.T) {
	keyer := func(input string) string {
		return input
	}

	// Create two limits: 9 per second and 21 per hour
	limitPerSecond := NewLimit(9, time.Second)
	limitPerHour := NewLimit(21, time.Hour)

	limiter := NewLimiter(keyer, limitPerSecond, limitPerHour)
	now := time.Now()

	for range 2 {
		// Test that we can make 9 requests within the first second (limited by per-second limit)
		for i := range limitPerSecond.Count {
			require.True(t, limiter.allow("test", now), "request %d should be allowed within per-second limit", i+1)
		}

		// The 10th request should be rejected due to per-second limit
		require.False(t, limiter.allow("test", now), "10th request should be rejected due to per-second limit")

		// Continue making requests until we hit the hourly limit
		for i := range limitPerHour.Count - limitPerSecond.Count {
			// Add a second between them to ensure we're not hitting the per-second limit
			now = now.Add(time.Second)
			require.True(t, limiter.allow("test", now), "request %d should be allowed within hourly limit", i+1)
		}

		// The next request should be rejected due to hourly limit, we've made 21 total
		require.False(t, limiter.allow("test", now), "22nd request should be rejected due to hourly limit")

		// Advance time by 1 hour to refill both buckets
		now = now.Add(time.Hour)
	}
}

func TestLimiter_Allow_MultipleLimits_MultipleBuckets(t *testing.T) {
	keyer := func(bucketID int) string {
		return fmt.Sprintf("bucket-%d", bucketID)
	}

	// Create two limits: 5 per second and 15 per hour
	limitPerSecond := NewLimit(5, time.Second)
	limitPerHour := NewLimit(15, time.Hour)
	limiter := NewLimiter(keyer, limitPerSecond, limitPerHour)
	const buckets = 7

	now := time.Now()

	for range 3 {
		// Test that each bucket can use its full per-second limit
		for bucketID := range buckets {
			for i := range limitPerSecond.Count {
				allowed := limiter.allow(bucketID, now)
				require.True(t, allowed, "bucket %d, request %d should be allowed within per-second limit", bucketID, i+1)
			}

			// The 6th request should be rejected due to per-second limit
			allowed := limiter.allow(bucketID, now)
			require.False(t, allowed, "bucket %d, 6th request should be rejected due to per-second limit", bucketID)
		}

		// Advance time by 1 second to refill per-second buckets
		now = now.Add(time.Second)
	}

	// At this point, each bucket has used 15 tokens (5 per second for 3 seconds)
	// This should hit the hourly limit for each bucket

	// Advance time by 1 second to refill per-second buckets
	now = now.Add(time.Second)

	// Try to make requests for each bucket - they should all be rejected due to hourly limit
	for bucketID := range buckets {
		allowed := limiter.allow(bucketID, now)
		require.False(t, allowed, "bucket %d should be rejected due to hourly limit after 15 requests", bucketID)
	}

	// Advance time by 1 hour to refill hourly buckets
	now = now.Add(time.Hour)

	// Test that each bucket can use its full per-second limit again after hourly refill
	for bucketID := range buckets {
		for i := range limitPerSecond.Count {
			allowed := limiter.allow(bucketID, now)
			require.True(t, allowed, "bucket %d, request %d should be allowed after hourly refill", bucketID, i+1)
		}

		// The 6th request should be rejected due to per-second limit
		allowed := limiter.allow(bucketID, now)
		require.False(t, allowed, "bucket %d, 6th request should be rejected after hourly refill", bucketID)
	}
}

func TestLimiter_Allow_MultipleLimits_MultipleBuckets_Concurrent(t *testing.T) {
	keyer := func(bucketID int) string {
		return fmt.Sprintf("bucket-%d", bucketID)
	}

	// Create two limits: 5 per second and 15 per hour
	limitPerSecond := NewLimit(5, time.Second)
	limitPerHour := NewLimit(15, time.Hour)
	limiter := NewLimiter(keyer, limitPerSecond, limitPerHour)
	const buckets = 7

	now := time.Now()

	for round := range 3 {
		// Test that each bucket can use its full per-second limit concurrently
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limitPerSecond.Count {
				wg.Add(1)
				go func(bucketID int, requestID int64) {
					defer wg.Done()
					allowed := limiter.allow(bucketID, now)
					require.True(t, allowed, "round %d, bucket %d, request %d should be allowed within per-second limit", round+1, bucketID, requestID+1)
				}(bucketID, i)
			}
		}
		wg.Wait()

		// The 6th request should be rejected due to per-second limit for each bucket
		for bucketID := range buckets {
			allowed := limiter.allow(bucketID, now)
			require.False(t, allowed, "round %d, bucket %d, 6th request should be rejected due to per-second limit", round+1, bucketID)
		}

		// Advance time by 1 second to refill per-second buckets
		now = now.Add(time.Second)
	}

	// At this point, each bucket has used 15 tokens (5 per second for 3 seconds)
	// This should hit the hourly limit for each bucket

	// Advance time by 1 second to refill per-second buckets
	now = now.Add(time.Second)

	// Try to make requests for each bucket concurrently - they should all be rejected due to hourly limit
	var wg sync.WaitGroup
	for bucketID := range buckets {
		wg.Add(1)
		go func(bucketID int) {
			defer wg.Done()
			allowed := limiter.allow(bucketID, now)
			require.False(t, allowed, "bucket %d should be rejected due to hourly limit after 15 requests", bucketID)
		}(bucketID)
	}
	wg.Wait()

	// Advance time by 1 hour to refill hourly buckets
	now = now.Add(time.Hour)

	// Test that each bucket can use its full per-second limit again after hourly refill, concurrently
	for bucketID := range buckets {
		for i := range limitPerSecond.Count {
			wg.Add(1)
			go func(bucketID int, requestID int64) {
				defer wg.Done()
				allowed := limiter.allow(bucketID, now)
				require.True(t, allowed, "bucket %d, request %d should be allowed after hourly refill", bucketID, requestID+1)
			}(bucketID, i)
		}
	}
	wg.Wait()

	// The 6th request should be rejected due to per-second limit for each bucket
	for bucketID := range buckets {
		allowed := limiter.allow(bucketID, now)
		require.False(t, allowed, "bucket %d, 6th request should be rejected after hourly refill", bucketID)
	}
}
