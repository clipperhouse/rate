package rate

import "time"

// Details is a struct that contains the details of a rate limit check.
type Details[TInput any, TKey comparable] struct {
	allow           bool
	tokensRequested int64
	tokensConsumed  int64
	tokensRemaining int64
	executionTime   time.Time
	retryAfter      time.Duration
	input           TInput
	key             TKey
}

// Allowed returns true if the request was allowed.
func (d Details[TInput, TKey]) Allowed() bool {
	return d.allow
}

// ExecutionTime returns the time the request was executed.
func (d Details[TInput, TKey]) ExecutionTime() time.Time {
	return d.executionTime
}

// Input returns the input that was used to create the bucket.
func (d Details[TInput, TKey]) Input() TInput {
	return d.input
}

// Key returns the key of the bucket that was used for the request.
func (d Details[TInput, TKey]) Key() TKey {
	return d.key
}

// TokensRequested returns the number of tokens that were requested for the request.
func (d Details[TInput, TKey]) TokensRequested() int64 {
	return d.tokensRequested
}

// TokensConsumed returns the number of tokens that were consumed for the request.
func (d Details[TInput, TKey]) TokensConsumed() int64 {
	return d.tokensConsumed
}

// TokensRemaining returns the number of remaining tokens in the bucket
func (d Details[TInput, TKey]) TokensRemaining() int64 {
	return d.tokensRemaining
}
