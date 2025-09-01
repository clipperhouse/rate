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
		ready := make(chan struct{})
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

				// Signal when we reach 10% of the buckets
				if atomic.AddInt64(&launched, 1) == count/10 {
					close(ready)
				}
			}(i)
		}

		// Wait for 10% of the buckets to be created before running GC,
		// try to induce some concurrency.

		<-ready
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

		// I've seen some flakiness here, returing 199 sometimes
		require.Equal(t, int64(200), bm.count(), "should have 200 buckets after deletion and GC")
	})

	t.Run("gc multiple concurrent calls", func(t *testing.T) {
		var bm bucketMap[string]
		const count int64 = 500

		limit := NewLimit(10, time.Second)
		executionTime := ntime.Now()

		// Create buckets first
		for i := range count {
			key := fmt.Sprintf("key%d", i)
			b := bm.loadOrStore(key, executionTime, limit)

			// Make 1/5 of the buckets not full by consuming tokens
			if i%5 == 0 {
				b.mu.Lock()
				b.consumeTokens(executionTime, limit, 1)
				b.mu.Unlock()
			}
		}

		require.Equal(t, count, bm.count(), "should have 500 buckets initially")

		// Test multiple goroutines calling gc() simultaneously
		const concurrency = 10
		var wg sync.WaitGroup
		var deleted int64

		wg.Add(concurrency)
		for range concurrency {
			go func() {
				defer wg.Done()
				d := bm.gc(func() ntime.Time {
					return executionTime
				})
				atomic.AddInt64(&deleted, d)
			}()
		}

		wg.Wait()

		// With 1/5 buckets not full, we expect 4/5 * 500 = 400 buckets to be deleted
		expectedDeleted := count * 4 / 5
		require.Equal(t, expectedDeleted, deleted, "should have deleted 400 full buckets")
		require.Equal(t, int64(100), bm.count(), "should have 100 buckets remaining (the non-full ones)")
	})

	t.Run("gc during rapid bucket creation", func(t *testing.T) {
		var bm bucketMap[string]

		limit := NewLimit(10, time.Second)
		executionTime := ntime.Now()

		// Start rapid bucket creation
		stop := make(chan struct{})
		ready := make(chan struct{})
		var wg sync.WaitGroup
		var bucketCount int64

		wg.Add(1)
		go func() {
			defer wg.Done()
			const minBuckets = 100
			for i := int64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
					key := fmt.Sprintf("rapid_key%d", i)
					bm.loadOrStore(key, executionTime, limit)

					// Signal when we reach the target number of buckets
					if atomic.AddInt64(&bucketCount, 1) == minBuckets {
						close(ready)
					}

					// Small delay to make it "rapid" but not overwhelming
					time.Sleep(time.Microsecond)
				}
			}
		}()

		// Wait for target number of buckets to be created before running GC
		<-ready

		// Run GC while buckets are being created rapidly
		deleted := bm.gc(func() ntime.Time {
			return executionTime
		})

		// Stop the rapid creation
		close(stop)
		wg.Wait()

		// Verify GC completed successfully
		require.GreaterOrEqual(t, deleted, int64(0), "GC should complete without errors")

		// Final cleanup
		deleted = bm.gc(func() ntime.Time {
			return executionTime
		})
		require.GreaterOrEqual(t, deleted, int64(0), "final GC should complete without errors")
	})
}

func BenchmarkBucketMap_GC(b *testing.B) {
	b.Run("all_full_buckets", func(b *testing.B) {
		limit := NewLimit(10, time.Second)
		executionTime := ntime.Now()
		const count int64 = 10000

		// Pre-create all the bucket specs and buckets
		specs := make([]bucketSpec[string], count)
		buckets := make([]*bucket, count)
		for i := range count {
			key := fmt.Sprintf("key%d", i)
			specs[i] = bucketSpec[string]{
				limit:   limit,
				userKey: key,
			}
			bucket := newBucket(executionTime, limit)
			buckets[i] = &bucket
		}

		b.ResetTimer()
		for b.Loop() {
			var bm bucketMap[string]

			// We need a fresh map for the benchmark,
			// but it's not what we want to measure.
			// Its overhead looks to be ~50% of the
			// total time, so we can subtract it.
			for i := range count {
				bm.m.Store(specs[i], buckets[i])
			}

			// Run GC
			bm.gc(func() ntime.Time {
				return executionTime
			})
		}
	})

	b.Run("mixed_buckets", func(b *testing.B) {
		limit := NewLimit(10, time.Second)
		executionTime := ntime.Now()
		const count int64 = 10000

		// Pre-create all the bucket specs and buckets
		specs := make([]bucketSpec[string], count)
		buckets := make([]*bucket, count)
		for i := range count {
			key := fmt.Sprintf("key%d", i)
			specs[i] = bucketSpec[string]{
				limit:   limit,
				userKey: key,
			}
			bucket := newBucket(executionTime, limit)
			// Make 1/4 of buckets non-full
			if i%4 == 0 {
				bucket.consumeTokens(executionTime, limit, 1)
			}
			buckets[i] = &bucket
		}

		b.ResetTimer()
		for b.Loop() {
			var bm bucketMap[string]

			// We need a fresh map for the benchmark,
			// but it's not what we want to measure.
			// Its overhead looks to be ~50% of the
			// total time, so we can subtract it.
			for i := range count {
				bm.m.Store(specs[i], buckets[i])
			}

			// Run GC
			bm.gc(func() ntime.Time {
				return executionTime
			})
		}
	})

	b.Run("few_buckets", func(b *testing.B) {
		limit := NewLimit(10, time.Second)
		executionTime := ntime.Now()
		const count int64 = 100

		// Pre-create all the bucket specs and buckets
		specs := make([]bucketSpec[string], count)
		buckets := make([]*bucket, count)
		for i := range count {
			key := fmt.Sprintf("key%d", i)
			specs[i] = bucketSpec[string]{
				limit:   limit,
				userKey: key,
			}
			bucket := newBucket(executionTime, limit)
			buckets[i] = &bucket
		}

		b.ResetTimer()
		for b.Loop() {
			var bm bucketMap[string]

			// We need a fresh map for the benchmark,
			// but it's not what we want to measure.
			// Its overhead looks to be ~50% of the
			// total time, so we can subtract it.
			for i := range count {
				bm.m.Store(specs[i], buckets[i])
			}

			// Run GC
			bm.gc(func() ntime.Time {
				return executionTime
			})
		}
	})

	b.Run("many_buckets", func(b *testing.B) {
		limit := NewLimit(10, time.Second)
		executionTime := ntime.Now()
		const count int64 = 1000000

		// Pre-create all the bucket specs and buckets
		specs := make([]bucketSpec[string], count)
		buckets := make([]*bucket, count)
		for i := range count {
			key := fmt.Sprintf("key%d", i)
			specs[i] = bucketSpec[string]{
				limit:   limit,
				userKey: key,
			}
			bucket := newBucket(executionTime, limit)
			buckets[i] = &bucket
		}

		b.ResetTimer()
		for b.Loop() {
			var bm bucketMap[string]

			// We need a fresh map for the benchmark,
			// but it's not what we want to measure.
			// Its overhead looks to be ~50% of the
			// total time, so we can subtract it.
			for i := range count {
				bm.m.Store(specs[i], buckets[i])
			}

			// Run GC
			bm.gc(func() ntime.Time {
				return executionTime
			})
		}
	})

	b.Run("map_creation_only", func(b *testing.B) {
		// The above benchmarks require creating a
		// fresh map for each iteration, which is
		// not what we want to measure. This benchmark
		// gives an idea of that overhead, which can
		// be subtracted from the other benchmarks.

		// Manual observation, on my machine, the
		// map creation accounts for about half
		// of the benchmark time. So perhaps
		// we can halve the ns/op above when
		// evaluating.
		limit := NewLimit(10, time.Second)
		executionTime := ntime.Now()
		const count int64 = 10000

		// Pre-create all the bucket specs and buckets
		specs := make([]bucketSpec[string], count)
		buckets := make([]*bucket, count)
		for i := range count {
			key := fmt.Sprintf("key%d", i)
			specs[i] = bucketSpec[string]{
				limit:   limit,
				userKey: key,
			}
			bucket := newBucket(executionTime, limit)
			buckets[i] = &bucket
		}

		b.ResetTimer()
		for b.Loop() {
			var bm bucketMap[string]

			// Just measure the map creation overhead
			// to inform the other benchmarks.
			for i := range count {
				bm.m.Store(specs[i], buckets[i])
			}
		}
	})
}
