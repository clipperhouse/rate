package rate

import (
	"context"
	"time"
)

// Limiter is a rate limiter that can be used to limit the rate of requests to a given key.
type Limiter[TInput any, TKey comparable] struct {
	keyer     Keyer[TInput, TKey]
	limit     Limit
	limitFunc LimitFunc[TInput]
	buckets   syncMap[bucketSpec[TKey], *bucket]
}

// Keyer is a function that takes an input and returns a bucket key.
type Keyer[TInput any, TKey comparable] func(input TInput) TKey

// bucketSpec is a key for the bucket map, which includes the limit and the user key.
// It is a composite key to ensure that each bucket is unique for a given limit and user key.
type bucketSpec[TKey comparable] struct {
	limit Limit
	// userKey is the result of calling the user-defined Keyer
	userKey TKey
}

// NewLimiter creates a new rate limiter
func NewLimiter[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limit Limit) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer: keyer,
		limit: limit,
	}
}

// NewLimiterFunc creates a new rate limiter with a dynamic limit function. Use this if you
// wish to apply a different limit based on the input, for example by URL path. The LimitFunc
// takes the same input type as the Keyer function.
func NewLimiterFunc[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limitFunc LimitFunc[TInput]) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer:     keyer,
		limitFunc: limitFunc,
	}
}

// Allow returns true if tokens are available for the given key.
// If true, it will consume a token from the key's bucket. If false,
// no token will be consumed.
func (r *Limiter[TInput, TKey]) Allow(input TInput) bool {
	return r.allow(input, time.Now())
}

func (r *Limiter[TInput, TKey]) getBucketSpec(input TInput) bucketSpec[TKey] {
	return bucketSpec[TKey]{
		limit:   r.getLimit(input),
		userKey: r.keyer(input),
	}
}

func (r *Limiter[TInput, TKey]) allow(input TInput, executionTime time.Time) bool {
	key := r.getBucketSpec(input)
	limit := key.limit
	b := r.buckets.loadOrStore(key, newBucket(executionTime, limit))

	return b.Allow(executionTime, limit)
}

// AllowWithDetails returns true if tokens are available for the given key,
// and details about the bucket and the execution time. You might
// use these details for logging, returning headers, etc.
//
// If true, it will consume a token from the key's bucket. If false,
// no token will be consumed.
func (r *Limiter[TInput, TKey]) AllowWithDetails(input TInput) (bool, Details[TInput, TKey]) {
	return r.allowWithDetails(input, time.Now())
}

func (r *Limiter[TInput, TKey]) allowWithDetails(input TInput, executionTime time.Time) (bool, Details[TInput, TKey]) {
	spec := r.getBucketSpec(input)
	limit := spec.limit

	b := r.buckets.loadOrStore(spec, newBucket(executionTime, limit))

	allowed, bucketTime := b.AllowWithDetails(executionTime, limit)

	return allowed, Details[TInput, TKey]{
		allowed:       allowed,
		executionTime: executionTime,
		limit:         limit,
		bucketTime:    bucketTime,
		bucketInput:   input,
		bucketKey:     spec.userKey,
	}
}

// Peek returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) Peek(input TInput) bool {
	return r.peek(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peek(input TInput, executionTime time.Time) bool {
	spec := r.getBucketSpec(input)
	limit := spec.limit
	b := r.buckets.loadOrReturn(spec, newBucket(executionTime, limit))

	return b.HasToken(executionTime, limit)
}

// PeekWithDetails returns true if tokens are available for the given key,
// and details about the bucket and the execution time. You might
// use these details for logging, returning headers, etc.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekWithDetails(input TInput) (bool, Details[TInput, TKey]) {
	return r.peekWithDetails(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peekWithDetails(input TInput, executionTime time.Time) (bool, Details[TInput, TKey]) {
	spec := r.getBucketSpec(input)
	limit := spec.limit

	b := r.buckets.loadOrReturn(spec, newBucket(executionTime, limit))
	allowed, bucketTime := b.HasTokenWithDetails(executionTime, limit)

	return allowed, Details[TInput, TKey]{
		allowed:       allowed,
		executionTime: executionTime,
		limit:         limit,
		bucketTime:    bucketTime,
		bucketInput:   input,
		bucketKey:     spec.userKey,
	}
}

// Wait will poll the [Limiter.Allow] method for a period of time,
// until it is cancelled by the passed context. It has the
// effect of adding latency to requests instead of refusing
// them immediately. Consider it graceful degradation.
//
// Wait will return true if a token becomes available prior to
// the context cancellation, and will consume a token. It will
// return false if not, and therefore not consume a token.
//
// Take care to create an appropriate context. You almost certainly
// want [context.WithTimeout] or [context.WithDeadline].
//
// You should be conservative, as Wait will introduce
// backpressure on your upstream systems -- connections
// may be held open longer, requests may queue in memory.
//
// A good starting place will be to timeout after waiting
// for one token. For example:
//
//	ctx := context.WithTimeout(ctx, limit.DurationPerToken())
func (r *Limiter[TInput, TKey]) Wait(ctx context.Context, input TInput) bool {
	return r.wait(ctx, input, time.Now())
}

func (r *Limiter[TInput, TKey]) wait(ctx context.Context, input TInput, executionTime time.Time) bool {
	return r.waitWithCancellation(
		input,
		executionTime,
		ctx.Deadline,
		ctx.Done,
	)
}

// waitWithCancellation is a more testable version of wait that accepts
// deadline and done functions instead of a context, allowing for deterministic testing.
func (r *Limiter[TInput, TKey]) waitWithCancellation(
	input TInput,
	executionTime time.Time,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	spec := r.getBucketSpec(input)
	limit := spec.limit
	b := r.buckets.loadOrStore(spec, newBucket(executionTime, limit))

	return b.waitWithCancellation(executionTime, limit, deadline, done)
}

func (r *Limiter[TInput, TKey]) getLimit(input TInput) Limit {
	if r.limitFunc != nil {
		return r.limitFunc(input)
	}
	return r.limit
}
