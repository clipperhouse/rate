package rate

import (
	"time"
)

// Debug is a struct that contains the details of a rate limit check for a single bucket.
// Used by debug APIs that return per-bucket information.
type Debug[TInput any, TKey comparable] struct {
	allowed         bool
	executionTime   btime
	input           TInput
	key             TKey
	limit           Limit
	tokensRequested int64
	tokensConsumed  int64
	tokensRemaining int64
	retryAfter      time.Duration
}

// Allowed returns true if the request was allowed.
func (d Debug[TInput, TKey]) Allowed() bool {
	return d.allowed
}

// Limit returns the limit that was used for the request.
func (d Debug[TInput, TKey]) Limit() Limit {
	return d.limit
}

// ExecutionTime returns the time the request was executed.
func (d Debug[TInput, TKey]) ExecutionTime() btime {
	return d.executionTime
}

// Input returns the input that was used to create the bucket.
func (d Debug[TInput, TKey]) Input() TInput {
	return d.input
}

// Key returns the key of the bucket that was used for the request.
func (d Debug[TInput, TKey]) Key() TKey {
	return d.key
}

// TokensRequested returns the number of tokens that were requested for the request.
func (d Debug[TInput, TKey]) TokensRequested() int64 {
	return d.tokensRequested
}

// TokensConsumed returns the number of tokens that were consumed for the request.
func (d Debug[TInput, TKey]) TokensConsumed() int64 {
	return d.tokensConsumed
}

// TokensRemaining returns the number of remaining tokens in the bucket
func (d Debug[TInput, TKey]) TokensRemaining() int64 {
	return d.tokensRemaining
}

// RetryAfter returns the duration after which the bucket may have refilled.
func (d Debug[TInput, TKey]) RetryAfter() time.Duration {
	return d.retryAfter
}
