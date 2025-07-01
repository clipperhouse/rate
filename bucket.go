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
		// If the bucket is old, it should not mistakenly be interpreted as having too many tokens
		cutoff := now.Add(-limit.Period)
		if b.time.Before(cutoff) {
			b.time = cutoff
		}
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

// hasToken checks if any tokens are available in the bucket
// ⚠️ assumes the caller has locked appropriately
func (b *bucket) hasToken(now time.Time, limit limit) bool {
	return !b.time.After(now.Add(-limit.durationPerToken))
}

// consumeToken removes one token from the bucket
// ⚠️ assumes the caller has locked appropriately
func (b *bucket) consumeToken(limit limit) {
	b.time = b.time.Add(limit.durationPerToken)
}

// remainingTokens returns the number of tokens remaining in the bucket
func (b *bucket) remainingTokens(now time.Time, limit limit) int64 {
	// If the bucket is old, it should not mistakenly be interpreted as having too many tokens
	time := b.time
	cutoff := now.Add(-limit.Period)
	if time.Before(cutoff) {
		return limit.Count
	}
	return int64(now.Sub(time) / limit.durationPerToken)
}
