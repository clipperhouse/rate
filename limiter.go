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

// NewLimiter creates a new rate limiter
func NewLimiter[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limit limit) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer:   keyer,
		buckets: make(map[TKey]*bucket),
		limit:   limit,
	}
}

// Allow returns true if tokens are available for the given key.
// If true, it will consume a token from the key's bucket. If false,
// no token will be consumed.
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

// Allow returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) Peek(input TInput) bool {
	return r.peek(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peek(input TInput, now time.Time) bool {
	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(now, r.limit)
	}

	return b.peek(now, r.limit)
}
