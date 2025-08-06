package rate

import "time"

// Peek returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) Peek(input TInput) bool {
	return r.peek(input, time.Now())
}

// peek returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) peek(input TInput, executionTime time.Time) bool {
	return r.peekN(input, executionTime, 1)
}

// PeekN returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) PeekN(input TInput, n int64) bool {
	return r.peekN(input, time.Now(), n)
}

// peek returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) peekN(input TInput, executionTime time.Time, n int64) bool {

	limits := r.getLimits(input)
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

// PeekWithDetails returns true if tokens are available for the given key,
// along with aggregated details optimized for setting response headers.
// This method avoids allocations and is suitable for performance-critical paths.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekWithDetails(input TInput) (bool, Details[TInput, TKey]) {
	return r.peekWithDetails(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peekWithDetails(input TInput, executionTime time.Time) (bool, Details[TInput, TKey]) {
	return r.peekNWithDetails(input, executionTime, 1)
}

// PeekNWithDetails returns true if `n` tokens are available for the given key,
// along with aggregated details optimized for setting response headers.
// This method avoids allocations and is suitable for performance-critical paths.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekNWithDetails(input TInput, n int64) (bool, Details[TInput, TKey]) {
	return r.peekNWithDetails(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) peekNWithDetails(input TInput, executionTime time.Time, n int64) (bool, Details[TInput, TKey]) {
	userKey := r.keyer(input)

	limits := r.getLimits(input)
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		return true, Details[TInput, TKey]{
			allowed:         true,
			tokensRequested: n,
			tokensConsumed:  0,
			executionTime:   executionTime,
			tokensRemaining: 0,
			retryAfter:      0,
			input:           input,
			key:             userKey,
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
		tokensRemaining: remainingTokens,
		retryAfter:      retryAfter,
		input:           input,
		key:             userKey,
	}
}

// PeekWithDebug returns true if tokens are available for the given key,
// and detailed debugging information about all bucket(s) and the execution time.
// You might use these details for logging, debugging, etc.
//
// Note: This method allocates and may be expensive for performance-critical paths.
// For setting response headers, consider using PeekWithDetails instead.
//
// No tokens are consumed.
func (r *Limiter[TInput, TKey]) PeekWithDebug(input TInput) (bool, []Debug[TInput, TKey]) {
	return r.peekWithDebug(input, time.Now())
}

func (r *Limiter[TInput, TKey]) peekWithDebug(input TInput, executionTime time.Time) (bool, []Debug[TInput, TKey]) {
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
func (r *Limiter[TInput, TKey]) PeekNWithDebug(input TInput, n int64) (bool, []Debug[TInput, TKey]) {
	return r.peekNWithDebug(input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) peekNWithDebug(input TInput, executionTime time.Time, n int64) (bool, []Debug[TInput, TKey]) {
	userKey := r.keyer(input)
	buckets, limits := r.getBucketsAndLimits(input, userKey, executionTime, false)

	details := make([]Debug[TInput, TKey], len(buckets))
	allowAll := true

	unlock := rLockBuckets(buckets)
	defer unlock()

	for i := range buckets {
		b := buckets[i]
		limit := limits[i]

		allow := b.hasTokens(executionTime, limit, n)
		allowAll = allowAll && allow

		details[i] = Debug[TInput, TKey]{
			allowed:         allow,
			executionTime:   executionTime,
			limit:           limit,
			tokensRequested: n,
			tokensConsumed:  0, // Never consume tokens in peek
			tokensRemaining: b.remainingTokens(executionTime, limit),
			input:           input,
			key:             r.keyer(input),
		}
	}

	return allowAll, details
}
