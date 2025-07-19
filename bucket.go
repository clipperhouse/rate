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

// allow returns true if there are available tokens in the bucket, and consumes a token if so.
// If false, no tokens were consumed.
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) allow(executionTime time.Time, limit Limit) bool {
	if b.hasToken(executionTime, limit) {
		b.consumeToken(executionTime, limit)
		return true
	}
	return false
}

// hasTokenWithDetails checks if there are available tokens and returns the bucket time.
// It is thread-safe and does not consume any tokens.
func (b *bucket) hasTokenWithDetails(executionTime time.Time, limit Limit) (bool, time.Time) {
	return b.hasToken(executionTime, limit), b.time
}

// hasToken checks if any tokens are available in the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) hasToken(executionTime time.Time, limit Limit) bool {
	return !b.time.After(executionTime.Add(-limit.durationPerToken))
}

// consumeToken removes one token from the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) consumeToken(executionTime time.Time, limit Limit) {
	b.checkCutoff(executionTime, limit)
	b.time = b.time.Add(limit.durationPerToken)
}

func (b *bucket) checkCutoff(executionTime time.Time, limit Limit) (updated bool) {
	// If the bucket is old, it should not mistakenly be interpreted as having too many tokens
	cutoff := executionTime.Add(-limit.period)
	if b.time.Before(cutoff) {
		b.time = cutoff
		return true
	}
	return false
}

// remainingTokens returns the number of tokens remaining in the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) remainingTokens(executionTime time.Time, limit Limit) int64 {
	// TODO: not sure I love this, maybe remainingTokens() should not mutate the bucket.
	b.checkCutoff(executionTime, limit)
	return remainingTokens(executionTime, b.time, limit)
}

// remainingTokens returns the number of tokens based on the difference
// between the execution time and the bucket time, divided by the duration per token.
func remainingTokens(executionTime time.Time, bucketTime time.Time, limit Limit) int64 {
	return int64(executionTime.Sub(bucketTime) / limit.durationPerToken)
}

// nextTokenTime returns the time when the next token might be available
// Note that concurrent access by other goroutines might consume tokens;
// treat nextTokenTime as a prediction, not a guarantee.
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) nextTokenTime(limit Limit) time.Time {
	return b.time.Add(limit.durationPerToken)
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
