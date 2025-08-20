package rate

import (
	"sync"

	"github.com/clipperhouse/ntime"
)

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
func (bm *bucketMap[TKey]) loadOrStore(userKey TKey, executionTime ntime.Time, limit Limit) *bucket {
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
