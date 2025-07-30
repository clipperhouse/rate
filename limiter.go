package rate

import (
	"context"
	"sync"
	"time"
)

// Limiter is a rate limiter that can be used to limit the rate of requests to a given key.
type Limiter[TInput any, TKey comparable] struct {
	keyer      Keyer[TInput, TKey]
	limits     []Limit
	limitFuncs []LimitFunc[TInput]
	buckets    syncMap[bucketSpec[TKey], *bucket]
	waitQueues syncMap[TKey, *sync.Mutex]
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
func NewLimiter[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limits ...Limit) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer:  keyer,
		limits: limits,
	}
}

// NewLimiterFunc creates a new rate limiter with a dynamic limit function. Use this if you
// wish to apply a different limit based on the input, for example by URL path. The LimitFunc
// takes the same input type as the Keyer function.
func NewLimiterFunc[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limitFuncs ...LimitFunc[TInput]) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer:      keyer,
		limitFuncs: limitFuncs,
	}
}

// Allow returns true if one or more tokens are available for the given key.
// If true, it will consume a token from the key's bucket. If false,
// no token will be consumed.
//
// If the Limiter has multiple limits, Allow will return true only if
// all limits allow the request, and one token will be consumed against
// each limit. If any limit would be exceeded, no token will be consumed
// against any limit.
func (r *Limiter[TInput, TKey]) Allow(input TInput) bool {
	return r.allow(input, time.Now())
}

// AllowN returns true if at least `n` tokens are available for the given key.
// If true, it will consume `n` tokens. If false, no token will be consumed.
//
// If the Limiter has multiple limits, AllowN will return true only if
// all limits allow the request, and `n` tokens will be consumed against
// each limit. If any limit would be exceeded, no token will be consumed
// against any limit.
func (r *Limiter[TInput, TKey]) AllowN(input TInput, n int64) bool {
	return r.allowN(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) getBucketSpecs(input TInput) []bucketSpec[TKey] {
	// limits and limitFuncs are mutually exclusive.
	if len(r.limitFuncs) > 0 {
		specs := make([]bucketSpec[TKey], len(r.limitFuncs))
		for i, limitFunc := range r.limitFuncs {
			specs[i] = bucketSpec[TKey]{
				limit:   limitFunc(input),
				userKey: r.keyer(input),
			}
		}
		return specs
	}

	if len(r.limits) > 0 {
		specs := make([]bucketSpec[TKey], len(r.limits))
		for i, limit := range r.limits {
			specs[i] = bucketSpec[TKey]{
				limit:   limit,
				userKey: r.keyer(input),
			}
		}
		return specs
	}

	return nil
}

// getBucketsAndLimits retrieves the buckets and limits for the given input and execution time.
// It returns "parallel" slices of buckets and limits; the i'th element in each slice corresponds
// to the same limit/bucket pair.
func (r *Limiter[TInput, TKey]) getBucketsAndLimits(input TInput, executionTime time.Time, persist bool) ([]*bucket, []Limit) {
	specs := r.getBucketSpecs(input)
	buckets := make([]*bucket, len(specs))
	limits := make([]Limit, len(specs))

	// buckets and limits must be the same length and ordering,
	// so the right limit is applied to the right bucket.

	for i, spec := range specs {
		var b *bucket
		if persist {
			b = r.buckets.loadOrStore(spec, newBucket(executionTime, spec.limit))
		} else {
			b = r.buckets.loadOrReturn(spec, newBucket(executionTime, spec.limit))
		}
		buckets[i] = b
		limits[i] = spec.limit
	}

	return buckets, limits
}

func lockBuckets(buckets []*bucket) (unlock func()) {
	for _, b := range buckets {
		b.mu.Lock()
	}
	return func() {
		for _, b := range buckets {
			b.mu.Unlock()
		}
	}
}

func rLockBuckets(buckets []*bucket) (unlock func()) {
	for _, b := range buckets {
		b.mu.RLock()
	}
	return func() {
		for _, b := range buckets {
			b.mu.RUnlock()
		}
	}
}

func (r *Limiter[TInput, TKey]) allow(input TInput, executionTime time.Time) bool {
	return r.allowN(input, executionTime, 1)
}

func (r *Limiter[TInput, TKey]) allowN(input TInput, executionTime time.Time, n int64) bool {
	// Allow must be true for all limits, a strict AND operation.
	// If any limit is not allowed, the overall allow is false and
	// no token is consumed from any bucket.

	buckets, limits := r.getBucketsAndLimits(input, executionTime, true)
	unlock := lockBuckets(buckets)
	defer unlock()

	// specs and buckets must be the same length and ordering,
	// so the right limit is applied to the right bucket.

	// Check if all buckets allowAll the token
	allowAll := true
	for i := range buckets {
		b := buckets[i]
		limit := limits[i]
		if !b.hasTokens(executionTime, limit, n) {
			allowAll = false
			break
		}
	}

	// Consume tokens only when all buckets allow
	if allowAll {
		for i := range buckets {
			b := buckets[i]
			limit := limits[i]
			b.consumeTokens(executionTime, limit, n)
		}
	}

	return allowAll
}

// AllowWithDetails returns true if a token is available for the given key,
// along with details about the bucket(s) and tokens. You might use these details for
// logging, returning headers, etc.
//
// If true, it will consume one token. If false, no token will be consumed.
//
// If the Limiter has multiple limits, AllowWithDetails will return true only if
// all limits allow the request, and one token will be consumed against
// each limit. If any limit would be exceeded, no token will be consumed
// against any limit.
func (r *Limiter[TInput, TKey]) AllowWithDetails(input TInput) (bool, []Details[TInput, TKey]) {
	return r.allowWithDetails(input, time.Now())
}

// AllowNWithDetails returns true if at least `n` tokens are available
// for the given key, along with details about the bucket(s), remaining tokens, etc.
// You might use these details for logging, returning headers, etc.
//
// If true, it will consume `n` tokens. If false, no token will be consumed.
//
// If the Limiter has multiple limits, AllowNWithDetails will return true only if
// all limits allow the request, and `n` tokens will be consumed against
// each limit. If any limit would be exceeded, no token will be consumed
// against any limit.
func (r *Limiter[TInput, TKey]) AllowNWithDetails(input TInput, n int64) (bool, []Details[TInput, TKey]) {
	return r.allowNWithDetails(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) allowWithDetails(input TInput, executionTime time.Time) (bool, []Details[TInput, TKey]) {
	return r.allowNWithDetails(input, executionTime, 1)
}

func (r *Limiter[TInput, TKey]) allowNWithDetails(input TInput, executionTime time.Time, n int64) (bool, []Details[TInput, TKey]) {
	// Allow must be true for all limits, a strict AND operation.
	// If any limit is not allowed, the overall allow is false and
	// no token is consumed from any bucket.

	buckets, limits := r.getBucketsAndLimits(input, executionTime, true)
	unlock := lockBuckets(buckets)
	defer unlock()

	details := make([]Details[TInput, TKey], len(buckets))

	allowAll := true
	for i := range buckets {
		b := buckets[i]
		limit := limits[i]
		allow := b.hasTokens(executionTime, limit, n)
		allowAll = allowAll && allow
		details[i] = Details[TInput, TKey]{
			allowed:         allow,
			executionTime:   executionTime,
			limit:           limit,
			tokensRequested: n,
			remainingTokens: b.remainingTokens(executionTime, limit),
			bucketInput:     input,
			bucketKey:       r.keyer(input),
		}
	}

	// Consume tokens only when all buckets allow
	if allowAll {
		for i := range buckets {
			b := buckets[i]
			limit := limits[i]
			b.consumeTokens(executionTime, limit, n)
			details[i].remainingTokens = b.remainingTokens(executionTime, limit)
			details[i].tokensConsumed = n
		}
	}

	return allowAll, details
}

// Peek returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) Peek(input TInput) bool {
	return r.peek(input, time.Now())
}

// PeekN returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) PeekN(input TInput, n int64) bool {
	return r.peekN(input, time.Now(), n)
}

// peek returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) peek(input TInput, executionTime time.Time) bool {
	return r.peekN(input, executionTime, 1)
}

// peek returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) peekN(input TInput, executionTime time.Time, n int64) bool {
	buckets, limits := r.getBucketsAndLimits(input, executionTime, false)

	unlock := rLockBuckets(buckets)
	defer unlock()
	for i := range buckets {
		b := buckets[i]
		limit := limits[i]
		if !b.hasTokens(executionTime, limit, n) {
			return false
		}
	}

	return true
}

// PeekWithDetails returns true if tokens are available for the given key,
// and details about the bucket and the execution time. You might
// use these details for logging, returning headers, etc.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekWithDetails(input TInput) (bool, []Details[TInput, TKey]) {
	return r.peekWithDetails(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peekWithDetails(input TInput, executionTime time.Time) (bool, []Details[TInput, TKey]) {
	return r.peekNWithDetails(input, executionTime, 1)
}

// PeekNWithDetails returns true if `n` tokens are available for the given key,
// along with details about the bucket and remaining tokens. You might
// use these details for logging, returning headers, etc.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekNWithDetails(input TInput, n int64) (bool, []Details[TInput, TKey]) {
	return r.peekNWithDetails(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) peekNWithDetails(input TInput, executionTime time.Time, n int64) (bool, []Details[TInput, TKey]) {
	buckets, limits := r.getBucketsAndLimits(input, executionTime, false)

	details := make([]Details[TInput, TKey], len(buckets))
	allowAll := true

	unlock := rLockBuckets(buckets)
	defer unlock()

	for i := range buckets {
		b := buckets[i]
		limit := limits[i]

		allow := b.hasTokens(executionTime, limit, n)
		allowAll = allowAll && allow

		details[i] = Details[TInput, TKey]{
			allowed:         allow,
			executionTime:   executionTime,
			limit:           limit,
			tokensRequested: n,
			tokensConsumed:  0, // Never consume tokens in peek
			remainingTokens: b.remainingTokens(executionTime, limit),
			bucketInput:     input,
			bucketKey:       r.keyer(input),
		}
	}

	return allowAll, details
}

// Wait will poll [Allow] for a period of time,
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
//
// WaitN offers best-effort ordering of requests, meaning that
// if multiple requests are waiting for the same bucket, they
// will likely be granted in the order they were requested.
func (r *Limiter[TInput, TKey]) Wait(ctx context.Context, input TInput) bool {
	return r.waitN(ctx, input, time.Now(), 1)
}

// WaitN will poll [Limiter.AllowN] for a period of time,
// until it is cancelled by the passed context. It has the
// effect of adding latency to requests instead of refusing
// them immediately. Consider it graceful degradation.
//
// WaitN will return true if `n` tokens become available prior to
// the context cancellation, and will consume `n` tokens. If not,
// it will return false, and therefore consume no tokens.
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
//
// WaitN offers best-effort ordering of requests, meaning that
// if multiple requests are waiting for the same bucket, they
// will likely be granted in the order they were requested.
func (r *Limiter[TInput, TKey]) WaitN(ctx context.Context, input TInput, n int64) bool {
	return r.waitN(ctx, input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) waitN(ctx context.Context, input TInput, executionTime time.Time, n int64) bool {
	return r.waitNWithCancellation(
		input,
		executionTime,
		n,
		ctx.Deadline,
		ctx.Done,
	)
}

func (r *Limiter[TInput, TKey]) waitWithCancellation(
	input TInput,
	startTime time.Time,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	return r.waitNWithCancellation(input, startTime, 1, deadline, done)
}

// waitWithCancellation is a more testable version of wait that accepts
// deadline and done functions instead of a context, allowing for deterministic testing.
func (r *Limiter[TInput, TKey]) waitNWithCancellation(
	input TInput,
	startTime time.Time,
	n int64,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	if r.allowN(input, startTime, n) {
		return true
	}

	// currentTime is an approximation of the real clock moving forward
	// it's imprecise because it depends on time.After below.
	// For testing purposes, I want startTime (execution time) to
	// be a parameter. The alternative is calling time.Now().
	currentTime := startTime

	key := r.keyer(input)
	grant := r.waitQueues.loadOrStore(key, &sync.Mutex{})

	for {
		// The goroutine at the front of the queue gets to try for a token first.
		grant.Lock()
		if r.allowN(input, currentTime, n) {
			grant.Unlock()
			return true
		}
		grant.Unlock()

		buckets, limits := r.getBucketsAndLimits(input, currentTime, true)
		unlock := rLockBuckets(buckets)

		var wait time.Duration
		for i, b := range buckets {
			limit := limits[i]
			nextTokenTime := b.nextTokensTime(limit, n)
			untilNext := nextTokenTime.Sub(currentTime)
			if i == 0 || untilNext > wait {
				wait = untilNext
			}
		}
		unlock()

		if wait < 0 {
			wait = 0
		}

		if deadline, ok := deadline(); ok {
			if deadline.Before(currentTime.Add(wait)) {
				return false
			}
		}

		select {
		case <-done():
			return false
		case <-time.After(wait):
			currentTime = currentTime.Add(wait)
		}
	}
}
