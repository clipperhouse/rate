package rate

import "time"

// Peek returns true if tokens are available for the given key,
// but without consuming any tokens.
func (r *Limiter[TInput, TKey]) Peek(input TInput) bool {
	return r.PeekN(input, 1)
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
	limits := r.getLimits(input)
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		return true
	}

	userKey := r.keyFunc(input)

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
		b := newBucket(executionTime, limit)
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
	return r.PeekNWithDetails(input, 1)
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
	userKey := r.keyFunc(input)

	limits := r.getLimits(input)
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		return true, Details[TInput, TKey]{
			allowed:         true,
			executionTime:   executionTime,
			tokensRequested: n,
			tokensConsumed:  0,
			tokensRemaining: 0,
			retryAfter:      0,
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
		b := newBucket(executionTime, limit)
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
		executionTime:   executionTime,
		tokensRequested: n,
		tokensConsumed:  0,
		tokensRemaining: remainingTokens,
		retryAfter:      retryAfter,
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
	return r.PeekNWithDebug(input, 1)
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

func (r *Limiter[TInput, TKey]) peekWithDebug(input TInput, executionTime time.Time) (bool, []Debug[TInput, TKey]) {
	return r.peekNWithDebug(input, executionTime, 1)
}

func (r *Limiter[TInput, TKey]) peekNWithDebug(input TInput, executionTime time.Time, n int64) (bool, []Debug[TInput, TKey]) {
	userKey := r.keyFunc(input)

	limits := r.getLimits(input)
	if len(limits) == 0 {
		// No limits defined, so we allow everything
		return true, []Debug[TInput, TKey]{
			{
				allowed:         true,
				executionTime:   executionTime,
				input:           input,
				key:             userKey,
				limit:           Limit{},
				tokensRequested: n,
				tokensConsumed:  0,
				tokensRemaining: 0,
				retryAfter:      0,
			},
		}
	}

	allowAll := true
	debugs := make([]Debug[TInput, TKey], 0, len(limits))

	// For peek operation, we can check each bucket individually
	// without needing to collect and lock them all together
	for _, limit := range limits {
		if b, ok := r.buckets.load(userKey, limit); ok {
			b.mu.RLock()

			allow := b.hasTokens(executionTime, limit, n)
			retryAfter := b.nextTokensTime(executionTime, limit, n).Sub(executionTime)
			debugs = append(debugs, Debug[TInput, TKey]{
				allowed:         allow,
				executionTime:   executionTime,
				input:           input,
				key:             userKey,
				limit:           limit,
				tokensRequested: n,
				tokensConsumed:  0, // Never consume tokens in peek
				tokensRemaining: b.remainingTokens(executionTime, limit),
				retryAfter:      max(0, retryAfter),
			})

			b.mu.RUnlock()
			allowAll = allowAll && allow

			continue
		}

		// Use stack-allocated bucket for missing buckets
		b := newBucket(executionTime, limit)
		allow := b.hasTokens(executionTime, limit, n)
		retryAfter := b.nextTokensTime(executionTime, limit, n).Sub(executionTime)
		debugs = append(debugs, Debug[TInput, TKey]{
			allowed:         allow,
			input:           input,
			key:             userKey,
			executionTime:   executionTime,
			limit:           limit,
			tokensRequested: n,
			tokensConsumed:  0,
			tokensRemaining: b.remainingTokens(executionTime, limit),
			retryAfter:      max(0, retryAfter),
		})
		allowAll = allowAll && allow
	}

	return allowAll, debugs
}
