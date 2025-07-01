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

	for range limit.Count {
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

func TestLimiter_AllowWithDetails(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()

	allow, details := limiter.allowWithDetails("test-details", now)
	require.True(t, allow)
	require.Equal(t, limit, details.Limit())
	require.Equal(t, now, details.ExecutionTime())
	require.Equal(t, "test-details", details.BucketKey())
	require.Equal(t, limit.Count-1, details.RemainingTokens())
}

func TestLimiter_Peek_SingleBucket(t *testing.T) {
	const key = "single-test-bucket"
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()

	// any number of peeks should be true
	for range limit.Count * 2 {
		require.True(t, limiter.peek(key, now))
	}

	// exhaust the bucket
	for range limit.Count {
		require.True(t, limiter.allow(key, now))
	}

	require.False(t, limiter.peek(key, now))

	// A token is ~111ms
	now = now.Add(time.Millisecond * 120)

	require.True(t, limiter.peek(key, now))
	require.True(t, limiter.allow(key, now))
}

func TestLimiter_Peek_MultipleBuckets(t *testing.T) {
	keyer := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
	now := time.Now()

	// any number of peeks should be true
	for bucketID := range buckets {
		for range limit.Count * 2 {
			actual := limiter.peek(bucketID, now)
			require.True(t, actual, now)
		}
	}

	// exhaust all buckets
	for bucketID := range buckets {
		for range limit.Count {
			actual := limiter.allow(bucketID, now)
			require.True(t, actual, "bucket %d should have tokens for allow", bucketID)
		}
	}

	for bucketID := range buckets {
		require.False(t, limiter.peek(bucketID, now), "bucket %d should be exhausted for peek", bucketID)
	}

	// A token is ~111ms, refill
	now = now.Add(time.Millisecond * 112)

	for bucketID := range buckets {
		require.True(t, limiter.peek(bucketID, now), "bucket %d should have tokens for peek", bucketID)
		require.True(t, limiter.allow(bucketID, now), "bucket %d should have tokens for allow", bucketID)
	}
}

func TestLimiter_Peek_MultipleBuckets_Concurrent(t *testing.T) {
	keyer := func(i int) string {
		return fmt.Sprintf("test-bucket-%d", i)
	}
	const buckets = 3
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)
	now := time.Now()

	// Concurrent peeks: all should be true initially
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limit.Count * 2 {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					actual := limiter.peek(bucketID, now)
					require.True(t, actual, "concurrent peek should be true for bucket %d (goroutine %d)", bucketID, processID)
				}(bucketID, i)
			}
		}
		wg.Wait()
	}

	// Exhaust all buckets
	for bucketID := range buckets {
		for range limit.Count {
			actual := limiter.allow(bucketID, now)
			require.True(t, actual, "bucket %d should have tokens for allow", bucketID)
		}
	}

	// Concurrent peeks: all should be false after exhaustion
	{
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limit.Count * 2 {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					actual := limiter.peek(bucketID, now)
					require.False(t, actual, "concurrent peek should be false for exhausted bucket %d (goroutine %d)", bucketID, processID)
				}(bucketID, i)
			}
		}
		wg.Wait()
	}

	// Refill: advance time and test concurrent peeks again, and throw in some allow
	{
		now = now.Add(limit.Period)
		var wg sync.WaitGroup
		for bucketID := range buckets {
			for i := range limit.Count {
				wg.Add(1)
				go func(bucketID int, processID int64) {
					defer wg.Done()
					peek := limiter.peek(bucketID, now)
					require.True(t, peek, "concurrent peek should be true for refilled bucket %d (goroutine %d)", bucketID, i)
					allow := limiter.allow(bucketID, now)
					require.True(t, allow, "concurrent allow should be true for refilled bucket %d (goroutine %d)", bucketID, i)
				}(bucketID, i)
			}
		}
		wg.Wait()
	}
}

func TestLimiter_PeekWithDetails(t *testing.T) {
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(9, time.Second)
	limiter := NewLimiter(keyer, limit)

	now := time.Now()

	allowed, details := limiter.peekWithDetails("test-details", now)
	require.True(t, allowed)
	require.Equal(t, limit, details.Limit())
	require.Equal(t, now, details.ExecutionTime())
	require.Equal(t, "test-details", details.BucketKey())
	require.Equal(t, limit.Count, details.RemainingTokens())
}
