package ratelimiter

import (
	"sync"
	"time"
)

type bucket struct {
	time time.Time
	mu   sync.Mutex
}

func newBucket(now time.Time, limit limit) *bucket {
	return &bucket{
		time: now.Add(-limit.Period),
	}
}

// allow returns true there are available tokens in the bucket, and consumes a token if so.
// If it returns false, no tokens were consumed.
func (b *bucket) allow(now time.Time, limit limit) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if enough time has passed such that there is at least one token
	allow := b.time.Before(now.Add(-limit.durationPerToken))
	if allow {
		b.consumeToken(limit)
	}
	return allow
}

func (b *bucket) consumeToken(limit limit) {
	b.time = b.time.Add(limit.durationPerToken)
}

// remainingTokens returns the number of tokens remaining in the bucket
func (b *bucket) remainingTokens(now time.Time, limit limit) int64 {
	return int64(now.Sub(b.time) / limit.durationPerToken)
}
