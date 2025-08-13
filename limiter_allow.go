package rate

import (
	"time"
)

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

func (r *Limiter[TInput, TKey]) allow(input TInput, executionTime time.Time) bool {
	return r.allowN(input, executionTime, 1)
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

func (r *Limiter[TInput, TKey]) allowN(input TInput, executionTime time.Time, n int64) bool {
	// Allow must be true for all limits, a strict AND operation.
	// If any limit is not allowed, the overall allow is false and
	// no token is consumed from any bucket.

	// Optimization for single-limit case, likely common
	if len(r.limits) == 1 {
		limit := r.limits[0]
		userKey := r.keyFunc(input)
		b := r.buckets.loadOrStore(userKey, executionTime, limit)

		b.mu.Lock()
		defer b.mu.Unlock()

		if !b.hasTokens(executionTime, limit, n) {
			return false
		}

		b.consumeTokens(executionTime, limit, n)
		return true
	}

	limits := r.getLimits(input)
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		return true
	}

	// Collect the buckets
	var buckets []*bucket

	// Optimization: use a stack allocation for small numbers of limits,
	// should be the common case
	const maxStackBuckets = 4
	if len(limits) <= maxStackBuckets {
		var stackBuckets [maxStackBuckets]*bucket
		buckets = stackBuckets[:0]
	} else {
		// Fall back to heap for larger cases
		buckets = make([]*bucket, 0, len(limits))
	}

	userKey := r.keyFunc(input)

	// We need to collect all buckets and lock them together
	for _, limit := range limits {
		b := r.buckets.loadOrStore(userKey, executionTime, limit)
		buckets = append(buckets, b)
		b.mu.Lock()
	}
	// ...and defer unlock
	defer func() {
		for _, b := range buckets {
			b.mu.Unlock()
		}
	}()

	// Check if all buckets allow
	for i, b := range buckets {
		limit := limits[i]
		if !b.hasTokens(executionTime, limit, n) {
			return false
		}
	}

	// Consume tokens only when all buckets allow
	for i := range buckets {
		b := buckets[i]
		limit := limits[i]
		b.consumeTokens(executionTime, limit, n)
	}

	return true
}

// AllowWithDetails returns true if a token is available for the given key,
// along with details for setting response headers.
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

func (r *Limiter[TInput, TKey]) allowWithDetails(input TInput, executionTime time.Time) (bool, Details[TInput, TKey]) {
	return r.allowNWithDetails(input, executionTime, 1)
}

// AllowNWithDetails returns true if at least `n` tokens are available
// for the given key, along with details for setting response headers.
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

func (r *Limiter[TInput, TKey]) allowNWithDetails(input TInput, executionTime time.Time, n int64) (bool, Details[TInput, TKey]) {
	// Allow must be true for all limits, a strict AND operation.
	// If any limit is not allowed, the overall allow is false and
	// no token is consumed from any bucket.
	userKey := r.keyFunc(input)

	limits := r.getLimits(input)
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		details := Details[TInput, TKey]{
			allowed:         true,
			executionTime:   executionTime,
			tokensRequested: n,
			tokensConsumed:  0,
			tokensRemaining: 0,
			retryAfter:      0,
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

		(See AllowWithDebug for per-bucket details.)

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
		executionTime:   executionTime,
		tokensRequested: n,
		tokensConsumed:  consumed,
		tokensRemaining: remainingTokens,
		retryAfter:      retryAfter,
	}

	return allowAll, details
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
func (r *Limiter[TInput, TKey]) AllowWithDebug(input TInput) (bool, []Debug[TInput, TKey]) {
	return r.allowWithDebug(input, time.Now())
}

func (r *Limiter[TInput, TKey]) allowWithDebug(input TInput, executionTime time.Time) (bool, []Debug[TInput, TKey]) {
	return r.allowNWithDebug(input, executionTime, 1)
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
func (r *Limiter[TInput, TKey]) AllowNWithDebug(input TInput, n int64) (bool, []Debug[TInput, TKey]) {
	return r.allowNWithDebug(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) allowNWithDebug(input TInput, executionTime time.Time, n int64) (bool, []Debug[TInput, TKey]) {
	// Allow must be true for all limits, a strict AND operation.
	// If any limit is not allowed, the overall allow is false and
	// no token is consumed from any bucket.

	userKey := r.keyFunc(input)

	limits := r.getLimits(input)
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		debugs := []Debug[TInput, TKey]{
			{
				allowed:         true,
				input:           input,
				key:             userKey,
				limit:           Limit{},
				executionTime:   executionTime,
				tokensRequested: n,
				tokensConsumed:  0,
				tokensRemaining: 0,
				retryAfter:      0,
			},
		}
		return true, debugs
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
		b.mu.Lock()
	}
	// ..and defer unlock
	defer func() {
		for _, b := range buckets {
			b.mu.Unlock()
		}
	}()

	debugs := make([]Debug[TInput, TKey], len(buckets))

	allowAll := true
	for i := range buckets {
		b := buckets[i]
		limit := limits[i]
		allow := b.hasTokens(executionTime, limit, n)
		allowAll = allowAll && allow

		// some of these fields can only be determined
		// after we know if we are allowing all, see below
		debugs[i] = Debug[TInput, TKey]{
			allowed:         allow,
			executionTime:   executionTime,
			input:           input,
			key:             userKey,
			limit:           limit,
			tokensRequested: n,
		}
	}

	// Consume tokens only when all buckets allow
	if allowAll {
		for i := range buckets {
			b := buckets[i]
			limit := limits[i]
			b.consumeTokens(executionTime, limit, n)
			debugs[i].tokensConsumed = n
			debugs[i].tokensRemaining = b.remainingTokens(executionTime, limit)
			debugs[i].retryAfter = 0
		}

		return true, debugs
	}

	// We didn't allow all, so no tokens to consume,
	// but need to update details
	for i := range buckets {
		b := buckets[i]
		limit := limits[i]
		debugs[i].tokensConsumed = 0
		debugs[i].tokensRemaining = b.remainingTokens(executionTime, limit)
		retryAfter := b.nextTokensTime(executionTime, limit, n).Sub(executionTime)
		debugs[i].retryAfter = max(0, retryAfter)
	}

	return false, debugs
}
