package rate

import (
	"sync"
	"time"
)

// syncMap is a typed wrapper around sync.Map for our specific use case
type syncMap[K comparable, V any] struct {
	m sync.Map
}

// loadOrStore returns the existing value for the key if present.
// Otherwise, it calls the factory function to create a new value, stores it, and returns it.
// This avoids creating the value unless it's actually needed.
func (sm *syncMap[K, V]) loadOrStore(key K, getter func() V) V {
	if loaded, ok := sm.m.Load(key); ok {
		return loaded.(V)
	}
	// Only create the value if we didn't find an existing one
	value := getter()
	actual, _ := sm.m.LoadOrStore(key, value)
	return actual.(V)
}

func (sm *syncMap[K, V]) count() int {
	count := 0
	sm.m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// delete removes a key from the map
func (sm *syncMap[K, V]) delete(key K) {
	sm.m.Delete(key)
}

// bucketMap is a specialized sync.Map for storing buckets to avoid allocations
type bucketMap[TKey comparable] struct {
	m sync.Map
}

// bucketSpec is a key for the bucket map, which includes the limit and the user key.
// It is a composite key to ensure that each bucket is unique for a given limit and user key.
type bucketSpec[TKey comparable] struct {
	limit Limit
	// userKey is the result of calling the user-defined Keyer
	userKey TKey
}

// loadOrStore returns the existing bucket for the key if present.
// Otherwise, it creates a new bucket, stores it, and returns it.
// This is specialized to avoid a closure allocation for the getter.
func (bm *bucketMap[TKey]) loadOrStore(userKey TKey, executionTime time.Time, limit Limit) *bucket {
	spec := bucketSpec[TKey]{
		limit:   limit,
		userKey: userKey,
	}

	if loaded, ok := bm.m.Load(spec); ok {
		return loaded.(*bucket)
	}
	// Only create the b if we didn't find an existing one
	b := newBucket(executionTime, limit)
	actual, _ := bm.m.LoadOrStore(spec, &b)
	return actual.(*bucket)
}

// loadOrGet returns the existing value for the key if present.
// Otherwise, it returns a new (temporary) value.
func (bm *bucketMap[TKey]) loadOrGet(userKey TKey, executionTime time.Time, limit Limit) *bucket {
	spec := bucketSpec[TKey]{
		limit:   limit,
		userKey: userKey,
	}
	loaded, ok := bm.m.Load(spec)
	if ok {
		return loaded.(*bucket)
	}
	b := newBucket(executionTime, limit)
	return &b
}

func (bm *bucketMap[TKey]) load(userKey TKey, limit Limit) (*bucket, bool) {
	spec := bucketSpec[TKey]{
		limit:   limit,
		userKey: userKey,
	}
	if loaded, ok := bm.m.Load(spec); ok {
		return loaded.(*bucket), true
	}
	return nil, false
}

func (bm *bucketMap[TKey]) count() int {
	count := 0
	bm.m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
