package rate

import "time"

// Details contains aggregated rate limit details across multiple buckets/limits,
// optimized for the common use case of setting response headers without expensive allocations.
type Details[TInput any, TKey comparable] struct {
	allowed         bool
	executionTime   time.Time
	tokensRequested int64
	tokensConsumed  int64
	tokensRemaining int64         // minimum across all buckets (fewest before denial)
	retryAfter      time.Duration // maximum wait time across all buckets
}

// Allowed returns true if the request was allowed.
func (d Details[TInput, TKey]) Allowed() bool {
	return d.allowed
}

// ExecutionTime returns the time the request was executed.
func (d Details[TInput, TKey]) ExecutionTime() time.Time {
	return d.executionTime
}

// TokensRequested returns the number of tokens that were requested for the request.
func (d Details[TInput, TKey]) TokensRequested() int64 {
	return d.tokensRequested
}

// TokensConsumed returns the number of tokens that were consumed for the request.
func (d Details[TInput, TKey]) TokensConsumed() int64 {
	return d.tokensConsumed
}

// TokensRemaining returns the minimum number of remaining tokens across all buckets.
// This represents "after using this many tokens, you will be denied".
func (d Details[TInput, TKey]) TokensRemaining() int64 {
	return d.tokensRemaining
}

// RetryAfter returns the duration after which all relevant buckets may have refilled.
func (d Details[TInput, TKey]) RetryAfter() time.Duration {
	return d.retryAfter
}
