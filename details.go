package rate

import "time"

// Details is a struct that contains the details of a rate limit check.
type Details[TInput any, TKey comparable] struct {
	allowed       bool
	limit         Limit
	executionTime time.Time
	bucketTime    time.Time
	bucketInput   TInput
	bucketKey     TKey
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

// RemainingTokens returns the number of remaining tokens in the bucket
func (d Details[TInput, TKey]) RemainingTokens() int64 {
	return remainingTokens(d.executionTime, d.bucketTime, d.limit)
}
