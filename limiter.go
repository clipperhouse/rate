package rate

import (
	"context"
	"time"
)

// Limiter is a rate limiter that can be used to limit the rate of requests to a given key.
type Limiter[TInput any, TKey comparable] struct {
	keyer      Keyer[TInput, TKey]
	limits     []Limit
	limitFuncs []LimitFunc[TInput]
	buckets    syncMap[bucketSpec[TKey], *bucket]
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

// Allow returns true if tokens are available for the given key.
// If true, it will consume a token from the key's bucket. If false,
// no token will be consumed.
func (r *Limiter[TInput, TKey]) Allow(input TInput) bool {
	return r.allow(input, time.Now())
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
	// Allow must be true for all limits, a strict AND operation.
	// If any limit is not allowed, the overall allow is false and
	// no token is consumed from any bucket.

	// TODO: n as a parameter
	const n int64 = 1

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

// AllowWithDetails returns true if tokens are available for the given key,
// and details about the bucket and the execution time. You might
// use these details for logging, returning headers, etc.
//
// If true, it will consume a token from the key's bucket. If false,
// no token will be consumed.
func (r *Limiter[TInput, TKey]) AllowWithDetails(input TInput) (bool, []Details[TInput, TKey]) {
	return r.allowWithDetails(input, time.Now())
}

func (r *Limiter[TInput, TKey]) allowWithDetails(input TInput, executionTime time.Time) (bool, []Details[TInput, TKey]) {
	// Allow must be true for all limits, a strict AND operation.
	// If any limit is not allowed, the overall allow is false and
	// no token is consumed from any bucket.

	// TODO: n as a parameter
	const n int64 = 1

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

// peek returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) peek(input TInput, executionTime time.Time) bool {
	// TODO: n as a parameter
	const n int64 = 1

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
	// TODO: n as a parameter
	const n int64 = 1

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
	startTime time.Time,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	// TODO: n as a parameter
	const n int64 = 1

	// "current" time is meant to be an approximation of the
	// delta between the start time and the real system clock.
	currentTime := startTime

	for {
		if r.allow(input, currentTime) {
			return true
		}

		// Optimization: find the best time to try again

		buckets, limits := r.getBucketsAndLimits(input, currentTime, true)

		unlock := rLockBuckets(buckets)
		wait := 100 * time.Millisecond // arbitrary default, is there a better way?
		for i := range buckets {
			b := buckets[i]
			limit := limits[i]
			nextToken := b.nextTokensTime(limit, n)
			untilNext := nextToken.Sub(currentTime)
			wait = min(wait, untilNext)
		}
		unlock()

		// early return if we can't possibly acquire a token before the context is done
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
