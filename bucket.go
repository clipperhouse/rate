package rate

import (
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
	time btime
	mu   sync.RWMutex
}

func newBucket(executionTime btime, limit Limit) bucket {
	return bucket{
		// subtracting the period represents filling it with tokens
		time: executionTime.Add(-limit.period),
	}
}

// hasTokens checks if there are at least `n` tokens in the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) hasTokens(executionTime btime, limit Limit, n int64) bool {
	cutoff := b.cutoff(executionTime, limit)
	// "not after" is "before or equal"
	return !cutoff.After(executionTime.Add(-limit.durationPerToken * time.Duration(n)))
}

// consumeTokens removes `n` tokens from the bucket
//
// consumeTokens does not check if there are enough tokens in the bucket;
// therefore, you can go into "debt" by consuming more tokens than are available.
//
// n can be negative, which has the effect of adding tokens to the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) consumeTokens(executionTime btime, limit Limit, n int64) {
	cutoff := b.cutoff(executionTime, limit)
	b.time = cutoff.Add(limit.durationPerToken * time.Duration(n))
}

// cutoff checks if the bucket is old, and if so, returns its
// maximum legitimate value, which is its "full" state.
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) cutoff(executionTime btime, limit Limit) btime {
	cutoff := executionTime.Add(-limit.period)
	if b.time.Before(cutoff) {
		return cutoff
	}
	return b.time
}

// remainingTokens returns the number of tokens remaining in the bucket
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) remainingTokens(executionTime btime, limit Limit) int64 {
	cutoff := b.cutoff(executionTime, limit)
	return remainingTokens(executionTime, cutoff, limit)
}

// remainingTokens returns the number of tokens based on the difference
// between the execution time and the bucket time, divided by the duration per token.
func remainingTokens(executionTime btime, bucketTime btime, limit Limit) int64 {
	return int64(executionTime.Sub(bucketTime) / limit.durationPerToken)
}

// nextTokensTime returns the earliest time when `n` tokens might be available,
// due to the passage of time.
//
// Note: concurrent access by other goroutines might consume (or add!) tokens;
// treat nextTokensTime as a prediction, not a guarantee.
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) nextTokensTime(executionTime btime, limit Limit, n int64) btime {
	cutoff := b.cutoff(executionTime, limit)
	return cutoff.Add(limit.durationPerToken * time.Duration(n))
}

// retryAfter returns the duration until `n` tokens might be available,
// due to the passage of time.
//
// Note "might", because concurrent access by other goroutines might
// consume (or add!) tokens. Treat retryAfter as a prediction, not a guarantee.
//
// It returns 0 if the bucket has enough tokens, rather than a spurious negative
// duration.
//
// ⚠️ caller is responsible for locking appropriately
func (b *bucket) retryAfter(executionTime btime, limit Limit, n int64) time.Duration {
	return max(0, b.nextTokensTime(executionTime, limit, n).Sub(executionTime))
}
