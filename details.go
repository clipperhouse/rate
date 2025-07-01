package rate

import "time"

type details[TKey comparable] struct {
	allowed       bool
	limit         limit
	executionTime time.Time
	bucketTime    time.Time
	bucketKey     TKey
}

func (d details[TKey]) Allowed() bool {
	return d.allowed
}

func (d details[TKey]) Limit() limit {
	return d.limit
}

func (d details[TKey]) ExecutionTime() time.Time {
	return d.executionTime
}

func (d details[TKey]) BucketKey() TKey {
	return d.bucketKey
}

func (d details[TKey]) RemainingTokens() int64 {
	return remainingTokens(d.executionTime, d.bucketTime, d.limit)
}
