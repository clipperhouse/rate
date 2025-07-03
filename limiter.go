package rate

import (
	"sync"
	"time"
)

// Keyer is a function that takes an input and returns a bucket key.
type Keyer[TInput any, TKey comparable] func(input TInput) TKey

// Limiter is a rate limiter that can be used to limit the rate of requests to a given key.
type Limiter[TInput any, TKey comparable] struct {
	keyer     Keyer[TInput, TKey]
	limit     Limit
	limitFunc LimitFunc[TInput]
	buckets   map[TKey]*bucket
	mu        sync.RWMutex
}

// NewLimiter creates a new rate limiter
func NewLimiter[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limit Limit) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer:   keyer,
		buckets: make(map[TKey]*bucket),
		limit:   limit,
	}
}

// NewLimiterFunc creates a new rate limiter with a dynamic limit function. Use this if you
// wish to apply a different limit based on the input, for example by URL path. The LimitFunc
// takes the same input type as the Keyer function.
func NewLimiterFunc[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limitFunc LimitFunc[TInput]) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer:     keyer,
		buckets:   make(map[TKey]*bucket),
		limitFunc: limitFunc,
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

	limit := r.getLimit(input)

	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(executionTime, limit)
		r.buckets[key] = b
	}

	return b.allow(executionTime, limit)
}

// AllowWithDetails returns true if tokens are available for the given key,
// and details about the bucket and the execution time. You might
// use these details for logging, returning headers, etc.
//
// If true, it will consume a token from the key's bucket. If false,
// no token will be consumed.
func (r *Limiter[TInput, TKey]) AllowWithDetails(input TInput) (bool, Details[TInput, TKey]) {
	return r.allowWithDetails(input, time.Now())
}

func (r *Limiter[TInput, TKey]) allowWithDetails(input TInput, executionTime time.Time) (bool, Details[TInput, TKey]) {
	r.mu.Lock()
	defer r.mu.Unlock()

	limit := r.getLimit(input)

	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(executionTime, limit)
		r.buckets[key] = b
	}

	allowed := b.allow(executionTime, limit)

	return allowed, Details[TInput, TKey]{
		allowed:       allowed,
		executionTime: executionTime,
		limit:         limit,
		bucketTime:    b.time,
		bucketInput:   input,
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

	limit := r.getLimit(input)

	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(executionTime, limit)
	}
	return b.peek(executionTime, limit)
}

// PeekWithDetails returns true if tokens are available for the given key,
// and details about the bucket and the execution time. You might
// use these details for logging, returning headers, etc.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekWithDetails(input TInput) (bool, Details[TInput, TKey]) {
	return r.peekWithDetails(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peekWithDetails(input TInput, executionTime time.Time) (bool, Details[TInput, TKey]) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	limit := r.getLimit(input)

	key := r.keyer(input)
	b, ok := r.buckets[key]
	if !ok {
		b = newBucket(executionTime, limit)
	}

	allowed := b.peek(executionTime, limit)

	return allowed, Details[TInput, TKey]{
		allowed:       allowed,
		executionTime: executionTime,
		limit:         limit,
		bucketTime:    b.time,
		bucketInput:   input,
		bucketKey:     key,
	}
}

func (r *Limiter[TInput, TKey]) getLimit(input TInput) Limit {
	if r.limitFunc != nil {
		return r.limitFunc(input)
	}
	return r.limit
}
