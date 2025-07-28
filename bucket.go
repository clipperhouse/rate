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

// allow returns true if there are at least `n` tokens available in the bucket,
// and consumes `n` tokens if so.
//
// If false, no tokens were consumed.
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) allow(executionTime time.Time, limit Limit, n int64) bool {
	if b.hasTokens(executionTime, limit, n) {
		b.consumeTokens(executionTime, limit, n)
		return true
	}
	return false
}

// hasTokens checks if there are at least `n` tokens in the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) hasTokens(executionTime time.Time, limit Limit, n int64) bool {
	// "not after" is "before or equal"
	return !b.time.After(executionTime.Add(-limit.durationPerToken * time.Duration(n)))
}

// consumeTokens removes `n` tokens from the bucket
//
// consumeTokens does not check if there are enough tokens in the bucket;
// therefore, you can go into "debt" by consuming more tokens than are available.
//
// n can be negative, which has the effect of adding tokens to the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) consumeTokens(executionTime time.Time, limit Limit, n int64) {
	b.checkCutoff(executionTime, limit)
	b.time = b.time.Add(limit.durationPerToken * time.Duration(n))
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

// nextTokensTime returns the earliest time when `n` tokens might be available,
// due to the passage of time.
//
// Note: concurrent access by other goroutines might consume (or add!) tokens;
// treat nextTokensTime as a prediction, not a guarantee.
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) nextTokensTime(limit Limit, n int64) time.Time {
	return b.time.Add(limit.durationPerToken * time.Duration(n))
}

// wait tries to acquire `n` tokens by polling b.allow(), until the context is cancelled
// or the allow succeeds.
//
// As with [allow], it returns true if a token was acquired, false if not.
func (b *bucket) wait(ctx context.Context, startTime time.Time, limit Limit, n int64) bool {
	return b.waitWithCancellation(
		startTime,
		limit,
		n,
		ctx.Deadline,
		ctx.Done,
	)
}

// waitWithCancellation tries to acquire `n` tokens by polling b.allow(), until the context
// is cancelled or the allow succeeds.
func (b *bucket) waitWithCancellation(
	startTime time.Time,
	limit Limit,
	n int64,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	// "current" time is meant to be an approximation of the
	// delta between the start time and the real system clock.
	currentTime := startTime

	for {
		b.mu.Lock()
		if b.allow(currentTime, limit, n) {
			b.mu.Unlock()
			return true
		}

		nextToken := b.nextTokensTime(limit, n)
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
