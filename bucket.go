package rate

import (
	"context"
	"sync"
	"time"
)

// bucket is a primitive for tracking tokens.
// It's only meaningful with a specific limit;
// using different limits with the same bucket
// will lead to incorrect behavior.
//
// We considered making limit a member of bucket,
// which might prevent mistakes. However, in
// anticipation of a large number of buckets,
// we choose to make the type as small as possible,
// trusting the caller ([Limiter], mainly) to do
// the right thing.
type bucket struct {
	time time.Time
	mu   sync.RWMutex
}

func newBucket(executionTime time.Time, limit Limit) *bucket {
	return &bucket{
		// subtracting the period represents filling it with tokens
		time: executionTime.Add(-limit.period),
	}
}

// Allow returns true if tokens are available in the bucket for the given execution time and limit.
// If a token is available, it returns true and consumes a token.
// If no token is available, it returns false and does not consume a token.
// It is thread-safe.
func (b *bucket) Allow(executionTime time.Time, limit Limit) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.allow(executionTime, limit)
}

/*
The private methods (mostly) are meant to be called from within
the public methods that handle locking. Public methods are
intended to be thread-safe, and safely callable.

Private methods offer primitives allowing higher-level logic,
such as in Limiter, for fine control.
*/

// allow returns true if there are available tokens in the bucket, and consumes a token if so.
// If false, no tokens were consumed.
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) allow(executionTime time.Time, limit Limit) bool {
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

// HasToken checks if there are available tokens in the bucket
func (b *bucket) HasToken(executionTime time.Time, limit Limit) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.hasToken(executionTime, limit)
}

// hasToken checks if any tokens are available in the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) hasToken(executionTime time.Time, limit Limit) bool {
	return !b.time.After(executionTime.Add(-limit.durationPerToken))
}

// ConsumeToken removes one token from the bucket
func (b *bucket) ConsumeToken(limit Limit) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.hasToken(time.Now(), limit) {
		b.consumeToken(limit)
	}
}

// consumeToken removes one token from the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) consumeToken(limit Limit) {
	b.time = b.time.Add(limit.durationPerToken)
}

// RemainingTokens returns the number of tokens remaining in the bucket
func (b *bucket) RemainingTokens(executionTime time.Time, limit Limit) int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return remainingTokens(executionTime, b.time, limit)
}

// remainingTokens returns the number of tokens remaining in the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) remainingTokens(executionTime time.Time, limit Limit) int64 {
	return remainingTokens(executionTime, b.time, limit)
}

// NextTokenTime returns the time when the next token might be available
// Note that concurrent access by other goroutines might consume tokens;
// treat NextTokenTime as a prediction, not a guarantee.
func (b *bucket) NextTokenTime(limit Limit) time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.nextTokenTime(limit)
}

// nextTokenTime returns the time when the next token might be available
// Note that concurrent access by other goroutines might consume tokens;
// treat nextTokenTime as a prediction, not a guarantee.
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) nextTokenTime(limit Limit) time.Time {
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
		b.mu.Lock()
		if b.allow(currentTime, limit) {
			b.mu.Unlock()
			return true
		}

		nextToken := b.nextTokenTime(limit)
		b.mu.Unlock()

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
