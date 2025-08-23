package rate

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/clipperhouse/ntime"
	"github.com/stretchr/testify/require"
)

func TestSyncMap_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return input
	}
	limit := NewLimit(100, time.Second)
	limiter := NewLimiter(keyFunc, limit)

	const concurrency = 100
	const ops = 1000

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for range ops {
				// Test Allow method
				limiter.Allow("test-key")

				// Test Peek method
				limiter.Peek("test-key")

				// Test AllowWithDetails method
				limiter.AllowWithDetails("test-key")

				// Test PeekWithDetails method
				limiter.PeekWithDetails("test-key")
			}
		}()
	}
	wg.Wait()

	// If we get here without panic or race conditions, the test passes
}

func TestBucketMap_LoadOrStore(t *testing.T) {
	t.Parallel()

	var bm bucketMap[string]
	limit1 := NewLimit(100, time.Second)
	limit2 := NewLimit(200, time.Second) // Different count
	executionTime := ntime.Now()

	// First call should create and store a new bucket
	bucket1 := bm.loadOrStore("key1", executionTime, limit1)
	require.NotNil(t, bucket1, "should return a bucket")

	// Second call with same key and limit should return the same bucket
	bucket2 := bm.loadOrStore("key1", executionTime, limit1)
	require.Equal(t, bucket1, bucket2, "should return the same bucket instance for same key and limit")

	// Third call with same key but different limit should return a different bucket
	bucket3 := bm.loadOrStore("key1", executionTime, limit2)
	require.False(t, bucket1 == bucket3, "should return different bucket for different limit")

	// Fourth call with different key should return a different bucket
	bucket4 := bm.loadOrStore("key2", executionTime, limit1)
	require.False(t, bucket1 == bucket4, "should return different bucket for different key")
}

func TestBucketMap_Load(t *testing.T) {
	t.Parallel()

	var bm bucketMap[string]
	limit := NewLimit(100, time.Second)
	executionTime := ntime.Now()

	// Load from empty map should return nil and false
	bucket, ok := bm.load("key1", limit)
	require.Nil(t, bucket, "should return nil for non-existent key")
	require.False(t, ok, "should return false for non-existent key")

	// Store a bucket first
	storedBucket := bm.loadOrStore("key1", executionTime, limit)

	// Now load should return the stored bucket
	bucket, ok = bm.load("key1", limit)
	require.Equal(t, storedBucket, bucket, "should return stored bucket")
	require.True(t, ok, "should return true for existing key")

	// Load with different limit should not find the bucket
	differentLimit := NewLimit(200, time.Second)
	bucket, ok = bm.load("key1", differentLimit)
	require.Nil(t, bucket, "should return nil for different limit")
	require.False(t, ok, "should return false for different limit")
}

func TestBucketMap_Count(t *testing.T) {
	t.Parallel()

	var bm bucketMap[string]
	limit1 := NewLimit(100, time.Second)
	limit2 := NewLimit(200, time.Second)
	executionTime := ntime.Now()

	// Empty map should have count 0
	require.Equal(t, int64(0), bm.count(), "empty map should have count 0")

	// Add buckets with different keys and limits
	for i := range 50 {
		key := fmt.Sprintf("key%d", i)
		bm.loadOrStore(key, executionTime, limit1)
	}

	require.Equal(t, int64(50), bm.count(), "should have 50 buckets after adding 50 different keys")

	// Add buckets with same keys but different limits
	for i := range 50 {
		key := fmt.Sprintf("key%d", i)
		bm.loadOrStore(key, executionTime, limit2)
	}

	require.Equal(t, int64(100), bm.count(), "should have 100 buckets after adding same keys with different limits")

	// Adding duplicate key-limit combinations should not increase count
	for i := range 25 {
		key := fmt.Sprintf("key%d", i)
		bm.loadOrStore(key, executionTime, limit1)
	}

	require.Equal(t, int64(100), bm.count(), "should still have 100 buckets after duplicate additions")
}

func TestBucketMap_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	var bm bucketMap[string]

	limit := NewLimit(100, time.Second)
	executionTime := ntime.Now()

	const concurrency = 100
	const ops = 1000

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := range concurrency {
		go func(goroutineID int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", goroutineID%10) // Use 10 different keys
			for range ops {
				// Test loadOrStore method
				bm.loadOrStore(key, executionTime, limit)

				// Test load method
				bm.load(key, limit)

				// Test count method
				bm.count()
			}
		}(i)
	}
	wg.Wait()

	// Verify final state
	finalCount := bm.count()
	require.True(t, finalCount <= 10, "should have at most 10 buckets (one per key), got %d", finalCount)
	require.True(t, finalCount > 0, "should have at least 1 bucket after concurrent operations")

	// If we get here without panic or race conditions, the test passes
}

func TestBucketMap_DifferentKeyTypes(t *testing.T) {
	t.Parallel()

	// Test with int keys
	var bm bucketMap[int]
	limit := NewLimit(100, time.Second)
	executionTime := ntime.Now()

	bucket1 := bm.loadOrStore(42, executionTime, limit)
	bucket2 := bm.loadOrStore(42, executionTime, limit)
	require.Equal(t, bucket1, bucket2, "should work with int keys")

	// Test with different int key
	bucket3 := bm.loadOrStore(43, executionTime, limit)
	require.False(t, bucket1 == bucket3, "different int keys should have different buckets")

	require.Equal(t, int64(2), bm.count(), "should have 2 buckets for 2 different int keys")
}

func TestBucketMap_GC(t *testing.T) {
	t.Parallel()

	t.Run("gc deletes full buckets", func(t *testing.T) {
		var bm bucketMap[string]
		const count int64 = 1000

		limit := NewLimit(10, time.Second)
		executionTime := ntime.Now()

		// Create a bunch of older buckets
		for i := range count {
			key := fmt.Sprintf("key%d", i)
			b := bm.loadOrStore(key, executionTime, limit)
			// 1/4 of the buckets will not be full
			if i%4 == 0 {
				b.consumeTokens(executionTime, limit, 1)
			}
		}

		require.Equal(t, count, bm.count(), "should have 1000 buckets after creating 1000 older buckets")

		// GC should delete 750 buckets full buckets, and not the 250 that are not full
		{
			deleted := bm.gc(func() ntime.Time {
				return executionTime
			})
			require.Equal(t, int64(750), deleted, "should have 750 deleted buckets, since 1/4 of the buckets are full")
			require.Equal(t, int64(250), bm.count(), "should have 250 buckets remaining after GC")
		}

		// Time passes
		executionTime = executionTime.Add(time.Second)

		// All remaining buckets are full now
		{
			deleted := bm.gc(func() ntime.Time {
				return executionTime
			})
			require.Equal(t, int64(250), deleted, "should have deleted the remaining 250 buckets")
			require.Equal(t, int64(0), bm.count(), "should have 0 buckets remaining after all deletions")
		}
	})

	t.Run("gc concurrent with reads and writes", func(t *testing.T) {
		var bm bucketMap[string]
		const count int64 = 1000

		limit := NewLimit(10, time.Second)
		executionTime := ntime.Now()

		// Create a bunch of buckets concurrently

		// Trying to get the timing right for a good test,
		// since a slow system like GitHub Actions seems
		// to take a while to launch goroutines.
		signal := make(chan struct{})
		launched := int64(0)

		var wg sync.WaitGroup
		for i := range count {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()

				time.Sleep(time.Millisecond)
				key := fmt.Sprintf("key%d", i)
				b := bm.loadOrStore(key, executionTime, limit)
				b.mu.Lock()
				// 1/5 of the buckets will not be full
				if i%5 == 0 {
					b.consumeTokens(executionTime, limit, 1)
				}
				b.mu.Unlock()

				// Signal when we reach 100 launched goroutines
				if atomic.AddInt64(&launched, 1) == 100 {
					close(signal)
				}
			}(i)
		}

		// Wait for 100 goroutines to launch before running GC,
		// try to induce some concurrency.

		<-signal
		bm.gc(func() ntime.Time {
			return executionTime
		})
		wg.Wait()

		// Expect that some, but not all, deletions have happened,
		// since there was concurrent creation of buckets.
		require.Less(t, bm.count(), count, "some deletions should have happened")

		// Now delete the remaining buckets, without concurrency
		bm.gc(func() ntime.Time {
			return executionTime
		})
		require.Equal(t, int64(200), bm.count(), "should have 200 buckets after deletion and GC")
	})
}
