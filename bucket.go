package rate

import (
	"sync"
	"time"
)

type bucket struct {
	time time.Time
	mu   sync.RWMutex
}

func newBucket(now time.Time, limit limit) *bucket {
	return &bucket{
		time: now.Add(-limit.Period),
	}
}

// allow returns true if there are available tokens in the bucket, and consumes a token if so.
// If it returns false, no tokens were consumed.
func (b *bucket) allow(now time.Time, limit limit) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.hasToken(now, limit) {
		b.consumeToken(limit)
		return true
	}
	return false
}

// peek returns true if there are available tokens in the bucket,
// but consumes no tokens and mutates no state.
func (b *bucket) peek(now time.Time, limit limit) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.hasToken(now, limit)
}

// hasToken assumes the caller has locked appropriately.
func (b *bucket) hasToken(now time.Time, limit limit) bool {
	return !b.time.After(now.Add(-limit.durationPerToken))
}

// Lock-free, assumes caller holds Lock.
func (b *bucket) consumeToken(limit limit) {
	b.time = b.time.Add(limit.durationPerToken)
}

// remainingTokens returns the number of tokens remaining in the bucket
func (b *bucket) remainingTokens(now time.Time, limit limit) int64 {
	time := b.time
	cutoff := now.Add(-limit.Period)
	if time.Before(cutoff) {
		return limit.Count
	}
	return int64(now.Sub(time) / limit.durationPerToken)
}
