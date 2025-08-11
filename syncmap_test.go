package rate

import (
	"fmt"
	"sync"
	"testing"
	"time"

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

func TestSyncMap_LoadOrStoreFunc(t *testing.T) {
	t.Parallel()

	sm := &syncMap[string, int]{}

	factoryCalls := 0
	factory := func() int {
		factoryCalls++
		return 42
	}

	// First call should call factory and store the value
	result1 := sm.loadOrStore("key1", factory)
	require.Equal(t, 42, result1, "should return factory value")
	require.Equal(t, 1, factoryCalls, "factory should be called once")

	// Second call with same key should NOT call factory
	result2 := sm.loadOrStore("key1", factory)
	require.Equal(t, 42, result2, "should return existing value")
	require.Equal(t, 1, factoryCalls, "factory should not be called again for existing key")

	// Third call with different key should call factory again
	result3 := sm.loadOrStore("key2", factory)
	require.Equal(t, 42, result3, "should return factory value for new key")
	require.Equal(t, 2, factoryCalls, "factory should be called for new key")
}

func TestSyncMap_Count(t *testing.T) {
	t.Parallel()

	sm := &syncMap[string, int]{}

	for range 2 {
		// should only be stored once despite multiple calls
		for key := range 101 {
			sm.loadOrStore(fmt.Sprint(key), func() int {
				return key
			})
		}
	}

	expected := 101
	actual := sm.count()
	require.Equal(t, expected, actual, "expected Count() to be accurate")
}

func TestBucketMap_LoadOrStore(t *testing.T) {
	t.Parallel()

	var bm bucketMap[string]
	limit1 := NewLimit(100, time.Second)
	limit2 := NewLimit(200, time.Second) // Different count
	executionTime := time.Now()

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

func TestBucketMap_LoadOrGet(t *testing.T) {
	t.Parallel()

	var bm bucketMap[string]
	limit := NewLimit(100, time.Second)
	executionTime := time.Now()

	// First call should return a temporary bucket (not stored)
	bucket1 := bm.loadOrGet("key1", executionTime, limit)
	require.NotNil(t, bucket1, "should return a bucket")

	// Second call should return another temporary bucket (different instance)
	// Use slightly different execution time to ensure different bucket instances
	bucket2 := bm.loadOrGet("key1", executionTime.Add(time.Nanosecond), limit)
	require.NotNil(t, bucket2, "should return a bucket")
	require.False(t, bucket1 == bucket2, "should return different temporary buckets when none stored")

	// Store a bucket first
	storedBucket := bm.loadOrStore("key1", executionTime, limit)

	// Now loadOrGet should return the stored bucket
	bucket3 := bm.loadOrGet("key1", executionTime, limit)
	require.Equal(t, storedBucket, bucket3, "should return stored bucket when available")
}

func TestBucketMap_Load(t *testing.T) {
	t.Parallel()

	var bm bucketMap[string]
	limit := NewLimit(100, time.Second)
	executionTime := time.Now()

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
	executionTime := time.Now()

	// Empty map should have count 0
	require.Equal(t, 0, bm.count(), "empty map should have count 0")

	// Add buckets with different keys and limits
	for i := range 50 {
		key := fmt.Sprintf("key%d", i)
		bm.loadOrStore(key, executionTime, limit1)
	}

	require.Equal(t, 50, bm.count(), "should have 50 buckets after adding 50 different keys")

	// Add buckets with same keys but different limits
	for i := range 50 {
		key := fmt.Sprintf("key%d", i)
		bm.loadOrStore(key, executionTime, limit2)
	}

	require.Equal(t, 100, bm.count(), "should have 100 buckets after adding same keys with different limits")

	// Adding duplicate key-limit combinations should not increase count
	for i := range 25 {
		key := fmt.Sprintf("key%d", i)
		bm.loadOrStore(key, executionTime, limit1)
	}

	require.Equal(t, 100, bm.count(), "should still have 100 buckets after duplicate additions")
}

func TestBucketMap_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	var bm bucketMap[string]

	limit := NewLimit(100, time.Second)
	executionTime := time.Now()

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

				// Test loadOrGet method
				bm.loadOrGet(key, executionTime, limit)

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
	executionTime := time.Now()

	bucket1 := bm.loadOrStore(42, executionTime, limit)
	bucket2 := bm.loadOrStore(42, executionTime, limit)
	require.Equal(t, bucket1, bucket2, "should work with int keys")

	// Test with different int key
	bucket3 := bm.loadOrStore(43, executionTime, limit)
	require.False(t, bucket1 == bucket3, "different int keys should have different buckets")

	require.Equal(t, 2, bm.count(), "should have 2 buckets for 2 different int keys")
}
