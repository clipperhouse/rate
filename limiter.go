package rate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Limiter is a rate limiter that can be used to limit the rate of requests to a given key.
type Limiter[TInput any, TKey comparable] struct {
	keyer      Keyer[TInput, TKey]
	limits     []Limit
	limitFuncs []LimitFunc[TInput]
	buckets    bucketMap[TKey]
	waiters    syncMap[TKey, *waiter]
}

// waiter represents a reservation queue for a specific key with reference counting
type waiter struct {
	mu    sync.Mutex
	count int64 // number of active waiters for this key
}

// increment atomically increments the waiter count and returns the new count
func (w *waiter) increment() int64 {
	return atomic.AddInt64(&w.count, 1)
}

// decrement atomically decrements the waiter count and returns the new count
func (w *waiter) decrement() int64 {
	return atomic.AddInt64(&w.count, -1)
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

func getWaiter() *waiter {
	return &waiter{}
}

// getWaiter atomically gets or creates a waiter entry and increments its reference count
func (r *Limiter[TInput, TKey]) getWaiter(key TKey) *waiter {
	waiter := r.waiters.loadOrStore(key, getWaiter)
	waiter.increment()
	return waiter
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

func (r *Limiter[TInput, TKey]) getBucketSpecs(input TInput, userKey TKey) []bucketSpec[TKey] {
	// limits and limitFuncs are mutually exclusive.
	if len(r.limitFuncs) > 0 {
		// Fast path for single limit function - avoid slice allocation
		if len(r.limitFuncs) == 1 {
			spec := bucketSpec[TKey]{
				limit:   r.limitFuncs[0](input),
				userKey: userKey,
			}
			return []bucketSpec[TKey]{spec}
		}

		specs := make([]bucketSpec[TKey], len(r.limitFuncs))
		for i, limitFunc := range r.limitFuncs {
			specs[i] = bucketSpec[TKey]{
				limit:   limitFunc(input),
				userKey: userKey,
			}
		}
		return specs
	}

	if len(r.limits) > 0 {
		// Fast path for single limit - avoid slice allocation
		if len(r.limits) == 1 {
			spec := bucketSpec[TKey]{
				limit:   r.limits[0],
				userKey: userKey,
			}
			return []bucketSpec[TKey]{spec}
		}

		specs := make([]bucketSpec[TKey], len(r.limits))
		for i, limit := range r.limits {
			specs[i] = bucketSpec[TKey]{
				limit:   limit,
				userKey: userKey,
			}
		}
		return specs
	}

	return nil
}

// getBucketsAndLimits retrieves the buckets and limits for the given input and execution time.
// It returns "parallel" slices of buckets and limits; the i'th element in each slice corresponds
// to the same limit/bucket pair.
func (r *Limiter[TInput, TKey]) getBucketsAndLimits(input TInput, userKey TKey, executionTime time.Time, persist bool) ([]*bucket, []Limit) {
	// Fast path for single static limit - avoid slice allocations
	if len(r.limits) == 1 && len(r.limitFuncs) == 0 {
		limit := r.limits[0]
		var b *bucket
		if persist {
			b = r.buckets.loadOrStore(userKey, executionTime, limit)
		} else {
			b = r.buckets.loadOrGet(userKey, executionTime, limit)
		}

		// Return slices with single elements - this still allocates but less than before
		return []*bucket{b}, []Limit{r.limits[0]}
	}

	specs := r.getBucketSpecs(input, userKey)
	buckets := make([]*bucket, len(specs))
	limits := make([]Limit, len(specs))

	// buckets and limits must be the same length and ordering,
	// so the right limit is applied to the right bucket.

	for i, spec := range specs {
		var b *bucket
		if persist {
			b = r.buckets.loadOrStore(userKey, executionTime, spec.limit)
		} else {
			b = r.buckets.loadOrGet(userKey, executionTime, spec.limit)
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

	// Collect the limits
	limits := r.limits
	if len(r.limitFuncs) > 0 {
		limits = make([]Limit, 0, len(r.limitFuncs))
		for _, limitFunc := range r.limitFuncs {
			limits = append(limits, limitFunc(input))
		}
	}
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		return true
	}

	userKey := r.keyer(input)

	for _, limit := range limits {
		b := r.buckets.loadOrStore(userKey, executionTime, limit)
		b.mu.RLock()
		allow := b.hasTokens(executionTime, limit, n)
		b.mu.RUnlock()
		if !allow {
			return false
		}
	}

	// Consume tokens only when all buckets allow
	for _, limit := range limits {
		b := r.buckets.loadOrStore(userKey, executionTime, limit)
		b.mu.Lock()
		b.consumeTokens(executionTime, limit, n)
		b.mu.Unlock()
	}

	return true
}

// AllowWithDebug returns true if a token is available for the given key,
// along with detailed debugging information about all bucket(s) and tokens.
// You might use these details for logging, debugging, etc.
//
// Note: This method allocates and may be expensive for performance-critical paths.
// For setting response headers, consider using AllowWithDetails instead.
//
// If true, it will consume one token. If false, no token will be consumed.
//
// If the Limiter has multiple limits, AllowWithDebug will return true only if
// all limits allow the request, and one token will be consumed against
// each limit. If any limit would be exceeded, no token will be consumed
// against any limit.
func (r *Limiter[TInput, TKey]) AllowWithDebug(input TInput) (bool, []DetailsDebug[TInput, TKey]) {
	return r.allowWithDebug(input, time.Now())
}

// AllowNWithDebug returns true if at least `n` tokens are available
// for the given key, along with detailed debugging information about all bucket(s),
// remaining tokens, etc. You might use these details for logging, debugging, etc.
//
// Note: This method allocates and may be expensive for performance-critical paths.
// For setting response headers, consider using AllowNWithDetails instead.
//
// If true, it will consume `n` tokens. If false, no token will be consumed.
//
// If the Limiter has multiple limits, AllowNWithDebug will return true only if
// all limits allow the request, and `n` tokens will be consumed against
// each limit. If any limit would be exceeded, no token will be consumed
// against any limit.
func (r *Limiter[TInput, TKey]) AllowNWithDebug(input TInput, n int64) (bool, []DetailsDebug[TInput, TKey]) {
	return r.allowNWithDebug(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) allowWithDebug(input TInput, executionTime time.Time) (bool, []DetailsDebug[TInput, TKey]) {
	return r.allowNWithDebug(input, executionTime, 1)
}

func (r *Limiter[TInput, TKey]) allowNWithDebug(input TInput, executionTime time.Time, n int64) (bool, []DetailsDebug[TInput, TKey]) {
	// Allow must be true for all limits, a strict AND operation.
	// If any limit is not allowed, the overall allow is false and
	// no token is consumed from any bucket.

	userKey := r.keyer(input)
	buckets, limits := r.getBucketsAndLimits(input, userKey, executionTime, true)
	unlock := lockBuckets(buckets)
	defer unlock()

	details := make([]DetailsDebug[TInput, TKey], len(buckets))

	allowAll := true
	for i := range buckets {
		b := buckets[i]
		limit := limits[i]
		allow := b.hasTokens(executionTime, limit, n)
		allowAll = allowAll && allow
		details[i] = DetailsDebug[TInput, TKey]{
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

// AllowWithDetails returns true if a token is available for the given key,
// along with aggregated details optimized for setting response headers.
// This method avoids allocations and is suitable for performance-critical paths.
//
// If true, it will consume one token. If false, no token will be consumed.
//
// If the Limiter has multiple limits, AllowWithDetails will return true only if
// all limits allow the request, and one token will be consumed against
// each limit. If any limit would be exceeded, no token will be consumed
// against any limit.
func (r *Limiter[TInput, TKey]) AllowWithDetails(input TInput) (bool, Details[TInput, TKey]) {
	return r.allowWithDetails(input, time.Now())
}

// AllowNWithDetails returns true if at least `n` tokens are available
// for the given key, along with aggregated details optimized for setting response headers.
// This method avoids allocations and is suitable for performance-critical paths.
//
// If true, it will consume `n` tokens. If false, no token will be consumed.
//
// If the Limiter has multiple limits, AllowNWithDetails will return true only if
// all limits allow the request, and `n` tokens will be consumed against
// each limit. If any limit would be exceeded, no token will be consumed
// against any limit.
func (r *Limiter[TInput, TKey]) AllowNWithDetails(input TInput, n int64) (bool, Details[TInput, TKey]) {
	return r.allowNWithDetails(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) allowWithDetails(input TInput, executionTime time.Time) (bool, Details[TInput, TKey]) {
	return r.allowNWithDetails(input, executionTime, 1)
}

func (r *Limiter[TInput, TKey]) allowNWithDetails(input TInput, executionTime time.Time, n int64) (bool, Details[TInput, TKey]) {
	// Allow must be true for all limits, a strict AND operation.
	// If any limit is not allowed, the overall allow is false and
	// no token is consumed from any bucket.
	userKey := r.keyer(input)

	// Collect the limits
	var limits []Limit

	// limits and limitFuncs are mutually exclusive.
	if len(r.limits) > 0 {
		limits = r.limits
	} else if len(r.limitFuncs) > 0 {
		limits = make([]Limit, 0, len(r.limitFuncs))
		for _, limitFunc := range r.limitFuncs {
			limits = append(limits, limitFunc(input))
		}
	}
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		details := Details[TInput, TKey]{
			allowed:         true,
			tokensRequested: n,
			tokensConsumed:  0,
			executionTime:   executionTime,
			remainingTokens: 0,
			retryAfter:      0,
			bucketInput:     input,
			bucketKey:       userKey,
		}
		return true, details
	}

	// Collect the buckets
	var buckets []*bucket

	// Optimization: use stack allocation for small numbers of limits,
	// should be the common case
	var bucketsArray [4]*bucket
	if len(limits) <= len(bucketsArray) {
		buckets = bucketsArray[:0]
	} else {
		// Fall back to heap for large cases
		buckets = make([]*bucket, 0, len(limits))
	}

	for _, limit := range limits {
		b := r.buckets.loadOrStore(userKey, executionTime, limit)
		buckets = append(buckets, b)
	}

	// Lock all buckets
	for _, b := range buckets {
		b.mu.Lock()
	}
	// ..and defer unlock
	defer func() {
		for _, b := range buckets {
			b.mu.Unlock()
		}
	}()

	/*
		In the case of multiple limits, we need to define what
		"remaining tokens" and "retry after" mean. There are `n`
		answers but we will return only one.

		(See AllowNWithDebug for per-bucket details.)

		So, let's answer the question of what is most useful to
		the caller.

		We define "remaining tokens" as the minimum number of
		tokens remaining across all buckets. This is the most
		conservative value and the one that is most useful for
		the caller.

		We define "retry after" as the maximum time-to-next-`n`-tokens
		across all buckets. This is the most conservative value
		and the one that is most useful for the caller.
	*/

	allowAll := buckets[0].hasTokens(executionTime, limits[0], n)
	remainingTokens := buckets[0].remainingTokens(executionTime, limits[0])
	retryAfter := buckets[0].nextTokensTime(executionTime, limits[0], n).Sub(executionTime)

	for i := 1; i < len(buckets); i++ {
		// note start at index 1 because we
		// checked index 0 above
		b := buckets[i]
		limit := limits[i]
		if !b.hasTokens(executionTime, limit, n) {
			allowAll = false
		}

		rt := b.remainingTokens(executionTime, limit)
		if rt < remainingTokens { // min
			remainingTokens = rt
		}

		// Calculate retry-after from this bucket's next tokens time
		r := b.nextTokensTime(executionTime, limit, n).Sub(executionTime)
		if r > retryAfter { // max
			retryAfter = r
		}
	}

	if remainingTokens < 0 {
		remainingTokens = 0
	}

	// Consume tokens only when all buckets allow
	consumed := int64(0)
	if allowAll {
		for i := range buckets {
			b := buckets[i]
			limit := limits[i]
			b.consumeTokens(executionTime, limit, n)

			// Update remaining tokens after consumption
			rt := b.remainingTokens(executionTime, limit)
			if rt < remainingTokens { // min
				remainingTokens = rt
			}
		}
		consumed = n

		// guardrail, still not sure if this is possible
		if remainingTokens < 0 {
			remainingTokens = 0
		}
	}

	// if the request was allowed, retryAfter will be negative
	if retryAfter < 0 {
		retryAfter = 0
	}

	details := Details[TInput, TKey]{
		allowed:         allowAll,
		tokensRequested: n,
		tokensConsumed:  consumed,
		executionTime:   executionTime,
		remainingTokens: remainingTokens,
		retryAfter:      retryAfter,
		bucketInput:     input,
		bucketKey:       userKey,
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
	// Collect the limits
	var limits []Limit

	// limits and limitFuncs are mutually exclusive.
	if len(r.limits) > 0 {
		limits = r.limits
	} else if len(r.limitFuncs) > 0 {
		limits = make([]Limit, 0, len(r.limitFuncs))
		for _, limitFunc := range r.limitFuncs {
			limits = append(limits, limitFunc(input))
		}
	}
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		return true
	}

	userKey := r.keyer(input)

	// For peek operation, we can check each bucket individually
	// without needing to collect and lock them all together
	for _, limit := range limits {
		if b, ok := r.buckets.load(userKey, limit); ok {
			b.mu.RLock()
			allowed := b.hasTokens(executionTime, limit, n)
			b.mu.RUnlock()
			if !allowed {
				return false
			}
			continue
		}

		// Use stack-allocated bucket for missing buckets

		// We might just return true here, since any new
		// bucket will allow, but going through the
		// formality just in case something changes
		b := bucket{
			time: executionTime.Add(-limit.period),
		}
		if !b.hasTokens(executionTime, limit, n) {
			return false
		}
	}
	return true
}

// PeekWithDebug returns true if tokens are available for the given key,
// and detailed debugging information about all bucket(s) and the execution time.
// You might use these details for logging, debugging, etc.
//
// Note: This method allocates and may be expensive for performance-critical paths.
// For setting response headers, consider using PeekWithDetails instead.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekWithDebug(input TInput) (bool, []DetailsDebug[TInput, TKey]) {
	return r.peekWithDebug(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peekWithDebug(input TInput, executionTime time.Time) (bool, []DetailsDebug[TInput, TKey]) {
	return r.peekNWithDebug(input, executionTime, 1)
}

// PeekNWithDebug returns true if `n` tokens are available for the given key,
// along with detailed debugging information about all bucket(s) and remaining tokens.
// You might use these details for logging, debugging, etc.
//
// Note: This method allocates and may be expensive for performance-critical paths.
// For setting response headers, consider using PeekNWithDetails instead.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekNWithDebug(input TInput, n int64) (bool, []DetailsDebug[TInput, TKey]) {
	return r.peekNWithDebug(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) peekNWithDebug(input TInput, executionTime time.Time, n int64) (bool, []DetailsDebug[TInput, TKey]) {
	userKey := r.keyer(input)
	buckets, limits := r.getBucketsAndLimits(input, userKey, executionTime, false)

	details := make([]DetailsDebug[TInput, TKey], len(buckets))
	allowAll := true

	unlock := rLockBuckets(buckets)
	defer unlock()

	for i := range buckets {
		b := buckets[i]
		limit := limits[i]

		allow := b.hasTokens(executionTime, limit, n)
		allowAll = allowAll && allow

		details[i] = DetailsDebug[TInput, TKey]{
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

// PeekWithDetails returns true if tokens are available for the given key,
// along with aggregated details optimized for setting response headers.
// This method avoids allocations and is suitable for performance-critical paths.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekWithDetails(input TInput) (bool, Details[TInput, TKey]) {
	return r.peekWithDetails(input, time.Now())
}

// PeekNWithDetails returns true if `n` tokens are available for the given key,
// along with aggregated details optimized for setting response headers.
// This method avoids allocations and is suitable for performance-critical paths.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekNWithDetails(input TInput, n int64) (bool, Details[TInput, TKey]) {
	return r.peekNWithDetails(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) peekWithDetails(input TInput, executionTime time.Time) (bool, Details[TInput, TKey]) {
	return r.peekNWithDetails(input, executionTime, 1)
}

func (r *Limiter[TInput, TKey]) peekNWithDetails(input TInput, executionTime time.Time, n int64) (bool, Details[TInput, TKey]) {
	userKey := r.keyer(input)

	// Collect the limits
	var limits []Limit

	// limits and limitFuncs are mutually exclusive.
	if len(r.limits) > 0 {
		limits = r.limits
	} else if len(r.limitFuncs) > 0 {
		limits = make([]Limit, 0, len(r.limitFuncs))
		for _, limitFunc := range r.limitFuncs {
			limits = append(limits, limitFunc(input))
		}
	}
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		return true, Details[TInput, TKey]{
			allowed:         true,
			tokensRequested: n,
			tokensConsumed:  0,
			executionTime:   executionTime,
			remainingTokens: 0,
			retryAfter:      0,
			bucketInput:     input,
			bucketKey:       userKey,
		}
	}

	allowAll := true
	remainingTokens := int64(-1) // Use -1 to indicate unset
	retryAfter := time.Duration(0)

	// For peek operation, we can check each bucket individually
	// without needing to collect and lock them all together
	for _, limit := range limits {
		if b, ok := r.buckets.load(userKey, limit); ok {
			b.mu.RLock()

			allowAll = allowAll && b.hasTokens(executionTime, limit, n)
			rt := b.remainingTokens(executionTime, limit)
			if remainingTokens == -1 || rt < remainingTokens { // min
				remainingTokens = rt
			}
			r := b.nextTokensTime(executionTime, limit, n).Sub(executionTime)
			if r > retryAfter { // max
				retryAfter = r
			}

			b.mu.RUnlock()

			continue
		}

		// Use stack-allocated bucket for missing buckets
		b := bucket{
			time: executionTime.Add(-limit.period),
		}
		allowAll = allowAll && b.hasTokens(executionTime, limit, n)
		rt := b.remainingTokens(executionTime, limit)
		if remainingTokens == -1 || rt < remainingTokens { // min
			remainingTokens = rt
		}
		r := b.nextTokensTime(executionTime, limit, n).Sub(executionTime)
		if r > retryAfter { // max
			retryAfter = r
		}
	}

	// if the request was allowed, retryAfter will be negative
	if retryAfter < 0 {
		retryAfter = 0
	}

	return allowAll, Details[TInput, TKey]{
		allowed:         allowAll,
		tokensRequested: n,
		tokensConsumed:  0,
		executionTime:   executionTime,
		remainingTokens: remainingTokens,
		retryAfter:      retryAfter,
		bucketInput:     input,
		bucketKey:       userKey,
	}
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
// Wait offers best-effort FIFO ordering of requests. Under sustained
// contention (when multiple requests wait longer than ~1ms), the Go runtime's
// mutex starvation mode ensures strict FIFO ordering. Under light load,
// ordering may be less strict but performance is optimized.
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
// WaitN offers best-effort FIFO ordering of requests. Under sustained
// contention (when multiple requests wait longer than ~1ms), the Go runtime's
// mutex starvation mode ensures strict FIFO ordering. Under light load,
// ordering may be less strict but performance is optimized.
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

	userKey := r.keyer(input)
	waiter := r.getWaiter(userKey)

	// Ensure cleanup happens when this waiter exits
	defer func() {
		// If no more waiters for this key, remove the entry to prevent memory leak,
		// reference-counted
		if waiter.decrement() == 0 {
			r.waiters.delete(userKey)
		}
	}()

	for {
		// The goroutine at the front of the queue gets to try for a token first.
		waiter.mu.Lock()

		// Try to get tokens while holding the waiter lock to prevent races
		if r.allowN(input, currentTime, n) {
			waiter.mu.Unlock()
			return true
		}

		// Calculate wait time while still holding waiter lock to maintain atomicity
		buckets, limits := r.getBucketsAndLimits(input, userKey, currentTime, true)
		unlock := rLockBuckets(buckets)

		// Find the earliest time that all buckets might have `n` tokens.
		// The bucket with the longest wait time will determine how long we wait,
		// because `allow` requires all buckets to have `n` tokens.
		var wait time.Duration
		for i, b := range buckets {
			limit := limits[i]
			nextTokensTime := b.nextTokensTime(currentTime, limit, n)
			untilNext := nextTokensTime.Sub(currentTime)
			if i == 0 || untilNext > wait {
				wait = untilNext
			}
		}
		unlock()

		// Release waiter lock after calculating wait time
		waiter.mu.Unlock()

		// guardrail, not sure if this is possible, but we don't want it.
		if wait < 0 {
			wait = 0
		}

		// if we can't possibly get a token, fail fast
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
