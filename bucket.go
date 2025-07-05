package rate

import (
	"context"
	"sync"
	"time"
)

type bucket struct {
	time time.Time
	mu   sync.RWMutex
}

func newBucket(executionTime time.Time, limit Limit) *bucket {
	return &bucket{
		time: executionTime.Add(-limit.period),
	}
}

// allow returns true if there are available tokens in the bucket, and consumes a token if so.
// If it returns false, no tokens were consumed.
func (b *bucket) allow(executionTime time.Time, limit Limit) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.hasToken(executionTime, limit) {
		// If the bucket is old, it should not mistakenly be interpreted as having too many tokens
		cutoff := executionTime.Add(-limit.period)
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
func (b *bucket) peek(executionTime time.Time, limit Limit) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.hasToken(executionTime, limit)
}

// hasToken checks if any tokens are available in the bucket
// ⚠️ assumes the caller has locked appropriately
func (b *bucket) hasToken(executionTime time.Time, limit Limit) bool {
	return !b.time.After(executionTime.Add(-limit.durationPerToken))
}

// consumeToken removes one token from the bucket
// ⚠️ assumes the caller has locked appropriately
func (b *bucket) consumeToken(limit Limit) {
	b.time = b.time.Add(limit.durationPerToken)
}

// remainingTokens returns the number of tokens remaining in the bucket
func (b *bucket) remainingTokens(executionTime time.Time, limit Limit) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return remainingTokens(executionTime, b.time, limit)
}

func (b *bucket) nextTokenTime(limit Limit) time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.time.Add(limit.durationPerToken)
}

// remainingTokens returns the number of tokens remaining in the bucket
func remainingTokens(executionTime time.Time, bucketTime time.Time, limit Limit) int64 {
	// If the bucket is old, it should not mistakenly be interpreted as having too many tokens
	cutoff := executionTime.Add(-limit.period)
	if bucketTime.Before(cutoff) {
		return limit.count
	}
	return int64(executionTime.Sub(bucketTime) / limit.durationPerToken)
}

// wait tries to acquire a token by polling b.allow(), until the context is cancelled
// or the allow succeeds.
//
// As with [allow], it returns true if a token was acquired, false if not.
func (b *bucket) wait(ctx context.Context, startTime time.Time, limit Limit) bool {
	return b.waitWithCancellation(
		startTime,
		limit,
		ctx.Deadline,
		ctx.Done,
	)
}

// waitWithCancellation is a more testable version of wait that accepts
// deadline and done functions instead of a context, allowing for deterministic testing.
func (b *bucket) waitWithCancellation(
	startTime time.Time,
	limit Limit,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	// "current" time is meant to be an approximation of the
	// delta between the start time and the real system clock.
	currentTime := startTime

	for {
		if b.allow(currentTime, limit) {
			return true
		}

		nextToken := b.nextTokenTime(limit)

		// early return if we can't possibly acquire a token before the context is done
		if deadline, ok := deadline(); ok {
			if deadline.Before(nextToken) {
				return false
			}
		}

		untilNext := nextToken.Sub(currentTime)

		// when is the soonest we might get a token?
		wait := min(untilNext, limit.durationPerToken)

		select {
		case <-done():
			return false
		case <-time.After(wait):
			currentTime = currentTime.Add(wait)
		}
	}
}
