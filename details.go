package rate

import "time"

// Details contains aggregated rate limit details across multiple buckets/limits.
type Details[TInput any, TKey comparable] struct {
	allowed         bool
	tokensRequested int64
	tokensConsumed  int64
	executionTime   time.Time
	remainingTokens int64         // minimum across all buckets (fewest before denial)
	retryAfter      time.Duration // maximum wait time across all buckets
	bucketInput     TInput
	bucketKey       TKey
}

// Allowed returns true if the request was allowed.
func (d Details[TInput, TKey]) Allowed() bool {
	return d.allowed
}

// ExecutionTime returns the time the request was executed.
func (d Details[TInput, TKey]) ExecutionTime() time.Time {
	return d.executionTime
}

// Input returns the input that was used to create the bucket.
func (d Details[TInput, TKey]) Input() TInput {
	return d.bucketInput
}

// Key returns the key passed by the caller for the request.
func (d Details[TInput, TKey]) Key() TKey {
	return d.bucketKey
}

// TokensRequested returns the number of tokens that were requested in this request.
func (d Details[TInput, TKey]) TokensRequested() int64 {
	return d.tokensRequested
}

// TokensConsumed returns the number of tokens that were consumed by the request.
// If the request was not allowed, or was a Peek, this will be 0.
func (d Details[TInput, TKey]) TokensConsumed() int64 {
	return d.tokensConsumed
}

// TokensRemaining returns the minimum number of remaining tokens across all buckets.
// This represents "after using this many tokens, you will be denied".
func (d Details[TInput, TKey]) TokensRemaining() int64 {
	return d.remainingTokens
}

// RetryAfter returns the duration after which all relevant buckets may have refilled.
// We say "may" because other requests may have consumed tokens concurrently.
func (d Details[TInput, TKey]) RetryAfter() time.Duration {
	return d.retryAfter
}

// Debug contains the details of a rate limit check for a single bucket.
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
