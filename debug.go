package rate

import "time"

// Debug is a struct that contains the details of a rate limit check.
type Debug[TInput any, TKey comparable] struct {
	allowed         bool
	limit           Limit
	tokensRequested int64
	tokensConsumed  int64
	executionTime   time.Time
	remainingTokens int64
	bucketInput     TInput
	bucketKey       TKey
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
func (d Debug[TInput, TKey]) ExecutionTime() time.Time {
	return d.executionTime
}

// BucketInput returns the input that was used to create the bucket.
func (d Debug[TInput, TKey]) BucketInput() TInput {
	return d.bucketInput
}

// BucketKey returns the key of the bucket that was used for the request.
func (d Debug[TInput, TKey]) BucketKey() TKey {
	return d.bucketKey
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
	return d.remainingTokens
}
