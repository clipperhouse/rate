package rate

import "time"

// Details is a struct that contains the details of a rate limit check.
type Details[TInput any, TKey comparable] struct {
	allowed         bool
	limit           Limit
	tokensRequested int64
	tokensConsumed  int64
	executionTime   time.Time
	remainingTokens int64
	bucketInput     TInput
	bucketKey       TKey
}

// DetailsAggregated contains aggregated rate limit details across multiple buckets/limits,
// optimized for the common use case of setting response headers without expensive allocations.
type DetailsAggregated[TInput any, TKey comparable] struct {
	allowed         bool
	tokensRequested int64
	tokensConsumed  int64
	executionTime   time.Time
	remainingTokens int64  // minimum across all buckets (fewest before denial)
	retryAfter      time.Duration // maximum wait time across all buckets
	bucketInput     TInput
	bucketKey       TKey
}

// Allowed returns true if the request was allowed.
func (d Details[TInput, TKey]) Allowed() bool {
	return d.allowed
}

// Limit returns the limit that was used for the request.
func (d Details[TInput, TKey]) Limit() Limit {
	return d.limit
}

// ExecutionTime returns the time the request was executed.
func (d Details[TInput, TKey]) ExecutionTime() time.Time {
	return d.executionTime
}

// BucketInput returns the input that was used to create the bucket.
func (d Details[TInput, TKey]) BucketInput() TInput {
	return d.bucketInput
}

// BucketKey returns the key of the bucket that was used for the request.
func (d Details[TInput, TKey]) BucketKey() TKey {
	return d.bucketKey
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
	return d.remainingTokens
}

// Allowed returns true if the request was allowed.
func (d DetailsAggregated[TInput, TKey]) Allowed() bool {
	return d.allowed
}

// ExecutionTime returns the time the request was executed.
func (d DetailsAggregated[TInput, TKey]) ExecutionTime() time.Time {
	return d.executionTime
}

// BucketInput returns the input that was used to create the bucket.
func (d DetailsAggregated[TInput, TKey]) BucketInput() TInput {
	return d.bucketInput
}

// BucketKey returns the key of the bucket that was used for the request.
func (d DetailsAggregated[TInput, TKey]) BucketKey() TKey {
	return d.bucketKey
}

// TokensRequested returns the number of tokens that were requested for the request.
func (d DetailsAggregated[TInput, TKey]) TokensRequested() int64 {
	return d.tokensRequested
}

// TokensConsumed returns the number of tokens that were consumed for the request.
func (d DetailsAggregated[TInput, TKey]) TokensConsumed() int64 {
	return d.tokensConsumed
}

// TokensRemaining returns the minimum number of remaining tokens across all buckets.
// This represents "after using this many tokens, you will be denied".
func (d DetailsAggregated[TInput, TKey]) TokensRemaining() int64 {
	return d.remainingTokens
}

// RetryAfter returns the duration after which all relevant buckets may have refilled.
func (d DetailsAggregated[TInput, TKey]) RetryAfter() time.Duration {
	return d.retryAfter
}
