package rate

import (
	"sync"
	"time"
)

type bucket struct {
	time time.Time
	mu   sync.RWMutex
}

func newBucket(executionTime time.Time, limit limit) *bucket {
	return &bucket{
		time: executionTime.Add(-limit.Period),
	}
}

// allow returns true if there are available tokens in the bucket, and consumes a token if so.
// If it returns false, no tokens were consumed.
func (b *bucket) allow(executionTime time.Time, limit limit) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.hasToken(executionTime, limit) {
		// If the bucket is old, it should not mistakenly be interpreted as having too many tokens
		cutoff := executionTime.Add(-limit.Period)
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
func (b *bucket) peek(executionTime time.Time, limit limit) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.hasToken(executionTime, limit)
}

// hasToken checks if any tokens are available in the bucket
// ⚠️ assumes the caller has locked appropriately
func (b *bucket) hasToken(executionTime time.Time, limit limit) bool {
	return !b.time.After(executionTime.Add(-limit.durationPerToken))
}

// consumeToken removes one token from the bucket
// ⚠️ assumes the caller has locked appropriately
func (b *bucket) consumeToken(limit limit) {
	b.time = b.time.Add(limit.durationPerToken)
}

// remainingTokens returns the number of tokens remaining in the bucket
func (b *bucket) remainingTokens(executionTime time.Time, limit limit) int64 {
	return remainingTokens(executionTime, b.time, limit)
}

// remainingTokens returns the number of tokens remaining in the bucket
func remainingTokens(executionTime time.Time, bucketTime time.Time, limit limit) int64 {
	// If the bucket is old, it should not mistakenly be interpreted as having too many tokens
	cutoff := executionTime.Add(-limit.Period)
	if bucketTime.Before(cutoff) {
		return limit.Count
	}
	return int64(executionTime.Sub(bucketTime) / limit.durationPerToken)
}
