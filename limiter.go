package rate

import (
	"sync"
	"time"
)

type Keyer[TInput any, TKey comparable] func(input TInput) TKey

type Limiter[TInput any, TKey comparable] struct {
	keyer   Keyer[TInput, TKey]
	limit   limit
	buckets map[TKey]*bucket
	mu      sync.Mutex
}

func NewLimiter[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limit limit) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer:   keyer,
		buckets: make(map[TKey]*bucket),
		limit:   limit,
	}
}

// Allow returns true if the input is allowed to proceed, false otherwise.
// In the case of multiple limits, all limits must be satisfied, or it will return false.
func (r *Limiter[TInput, TKey]) Allow(input TInput) bool {
	return r.allow(input, time.Now())
}

func (r *Limiter[TInput, TKey]) allow(input TInput, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(now, r.limit)
		r.buckets[key] = b
	}

	return b.allow(now, r.limit)
}
