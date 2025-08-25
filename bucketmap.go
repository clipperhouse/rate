package rate

import (
	"sync"

	"github.com/clipperhouse/ntime"
)

// bucketMap is a specialized sync.Map for storing buckets
type bucketMap[TKey comparable] struct {
	m  sync.Map
	mu sync.RWMutex
}

// bucketSpec is a key for the bucket map, which includes the limit and the user key.
// It is a composite key to ensure that each bucket is unique for a given limit and user key.
type bucketSpec[TKey comparable] struct {
	limit Limit
	// userKey is the result of calling the user-defined KeyFunc
	userKey TKey
}

// loadOrStore returns the existing bucket for the key if present.
// Otherwise, it creates a new bucket, stores it, and returns it.
func (bm *bucketMap[TKey]) loadOrStore(userKey TKey, executionTime ntime.Time, limit Limit) *bucket {
	spec := bucketSpec[TKey]{
		limit:   limit,
		userKey: userKey,
	}

	if loaded, ok := bm.m.Load(spec); ok {
		return loaded.(*bucket)
	}
	// Only create the bucket if we didn't find an existing one
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

func (bm *bucketMap[TKey]) count() int64 {
	count := int64(0)
	bm.m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

// gc deletes buckets that are full using mark-and-sweep
func (bm *bucketMap[TKey]) gc(timeFunc func() ntime.Time) (deleted int64) {
	// Phase 1: Mark buckets for deletion
	toDelete := make([]bucketSpec[TKey], 0)

	bm.m.Range(func(key, value any) bool {
		spec := key.(bucketSpec[TKey])
		b := value.(*bucket)
		b.mu.Lock()
		if b.isFull(timeFunc(), spec.limit) {
			toDelete = append(toDelete, spec)
		}
		b.mu.Unlock()
		return true
	})

	// Phase 2: Delete marked buckets atomically
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, spec := range toDelete {
		if _, loaded := bm.m.LoadAndDelete(spec); loaded {
			deleted++
		}
	}

	return deleted
}
