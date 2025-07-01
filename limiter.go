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
	mu      sync.RWMutex
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

func (r *Limiter[TInput, TKey]) allow(input TInput, executionTime time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(executionTime, r.limit)
		r.buckets[key] = b
	}

	return b.allow(executionTime, r.limit)
}

// AllowWithDetails returns true if tokens are available for the given key,
// and details about the bucket and the execution time. You might
// use these details for logging, returning headers, etc.
//
// If true, it will consume a token from the key's bucket. If false,
// no token will be consumed.
func (r *Limiter[TInput, TKey]) AllowWithDetails(input TInput) (bool, details[TKey]) {
	return r.allowWithDetails(input, time.Now())
}

func (r *Limiter[TInput, TKey]) allowWithDetails(input TInput, executionTime time.Time) (bool, details[TKey]) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(executionTime, r.limit)
		r.buckets[key] = b
	}

	allowed := b.allow(executionTime, r.limit)

	return allowed, details[TKey]{
		allowed:       allowed,
		executionTime: executionTime,
		limit:         r.limit,
		bucketTime:    b.time,
		bucketKey:     key,
	}
}

// Allow returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) Peek(input TInput) bool {
	return r.peek(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peek(input TInput, executionTime time.Time) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(executionTime, r.limit)
	}
	return b.peek(executionTime, r.limit)
}

// PeekWithDetails returns true if tokens are available for the given key,
// and details about the bucket and the execution time. You might
// use these details for logging, returning headers, etc.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekWithDetails(input TInput) (bool, details[TKey]) {
	return r.peekWithDetails(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peekWithDetails(input TInput, executionTime time.Time) (bool, details[TKey]) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(executionTime, r.limit)
	}

	allowed := b.peek(executionTime, r.limit)

	return allowed, details[TKey]{
		allowed:       allowed,
		executionTime: executionTime,
		limit:         r.limit,
		bucketTime:    b.time,
		bucketKey:     key,
	}
}
