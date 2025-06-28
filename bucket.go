package ratelimiter

import (
	"sync"
	"time"
)

type bucket struct {
	time  time.Time
	mutex sync.Mutex
}

func NewBucket(now time.Time, limit limit) bucket {
	return bucket{
		time: now.Add(-limit.Period),
	}
}

// Allow returns true there are available tokens in the bucket
func (b *bucket) Allow(now time.Time, limit limit) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// Check if enough time has passed such that there is at least one token
	allow := b.time.Before(now.Add(-limit.DurationPerToken))
	if allow {
		b.consumeToken(limit)
	}
	return allow
}

// ConsumeToken consumes a token from the bucket
func (b *bucket) ConsumeToken(limit limit) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.consumeToken(limit)
}

func (b *bucket) consumeToken(limit limit) {
	b.time = b.time.Add(limit.DurationPerToken)
}

// RemainingTokens returns the number of tokens remaining in the bucket
func (b *bucket) RemainingTokens(now time.Time, limit limit) int64 {
	return int64(now.Sub(b.time) / limit.DurationPerToken)
}
