package rate

import (
	"sync"
	"time"
)

type Keyer[TInput any, TKey comparable] func(input TInput) TKey

type Limiter[TInput any, TKey comparable] struct {
	keyer        Keyer[TInput, TKey]
	limitBuckets []limitBuckets[TKey]
	mu           sync.Mutex
}

type limitBuckets[TKey comparable] struct {
	mu      sync.Mutex
	limit   limit
	buckets map[TKey]*bucket
}

func NewLimiter[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limits ...limit) *Limiter[TInput, TKey] {
	lbs := make([]limitBuckets[TKey], len(limits))
	for i := range limits {
		lbs[i] = limitBuckets[TKey]{
			limit:   limits[i],
			buckets: make(map[TKey]*bucket),
		}
	}
	return &Limiter[TInput, TKey]{
		keyer:        keyer,
		limitBuckets: lbs,
	}
}

// Allow returns true if the input is allowed to proceed, false otherwise.
// In the case of multiple limits, all limits must be satisfied, or it will return false.
func (r *Limiter[TInput, TKey]) Allow(input TInput) bool {
	return r.allow(input, time.Now())
}

func (r *Limiter[TInput, TKey]) allow(input TInput, now time.Time) bool {
	key := r.keyer(input)

	r.mu.Lock()
	defer r.mu.Unlock()

	for i := range r.limitBuckets {
		lb := &r.limitBuckets[i]
		lb.mu.Lock()

		b, ok := lb.buckets[key]
		if !ok {
			b = newBucket(now, lb.limit)
			lb.buckets[key] = b
		}

		allow := b.allow(now, lb.limit)
		lb.mu.Unlock()

		if !allow {
			return false
		}
	}

	return true
}
