package ratelimiter

import (
	"sync"
	"time"
)

type Keyer[TInput any, TKey comparable] func(input TInput) TKey

type RateLimiter[TInput any, TKey comparable] struct {
	keyer   Keyer[TInput, TKey]
	buckets map[TKey]*bucket
	limit   limit
	mu      sync.Mutex
}

func NewRateLimiter[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limit limit) *RateLimiter[TInput, TKey] {
	return &RateLimiter[TInput, TKey]{
		keyer:   keyer,
		buckets: make(map[TKey]*bucket),
		limit:   limit,
	}
}

func (l *RateLimiter[TInput, TKey]) Allow(input TInput, now time.Time) bool {
	key := l.keyer(input)

	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		b = NewBucket(now, l.limit)
		l.buckets[key] = b
	}

	return b.Allow(now, l.limit)
}
