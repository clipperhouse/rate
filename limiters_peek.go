package rate

import (
	"time"

	"github.com/clipperhouse/rate/ntime"
)

// peekN returns true if tokens are available for the given input across all limiters,
// but without consuming any tokens. This method efficiently checks multiple limiters
// by testing each one individually and returning early if any limit is exceeded.
func (rs *Limiters[TInput, TKey]) peekN(input TInput, executionTime ntime.Time, n int64) bool {
	switch len(rs.limiters) {
	case 0:
		return true
	case 1:
		return rs.limiters[0].peekN(input, executionTime, n)
	}

	// For multiple limiters, check each one individually
	// This is more efficient than collecting all buckets since we can return early
	for _, r := range rs.limiters {
		limits := r.getLimits(input)
		if len(limits) == 0 {
			continue // No limits for this limiter, so it allows everything
		}

		userKey := r.keyFunc(input)

		// Check each limit for this limiter
		for _, limit := range limits {
			if b, ok := r.buckets.load(userKey, limit); ok {
				b.mu.RLock()
				allowed := b.hasTokens(executionTime, limit, n)
				b.mu.RUnlock()
				if !allowed {
					return false // Early return if any limit is exceeded
				}
				continue
			}

			// Use stack-allocated bucket for missing buckets
			b := newBucket(executionTime, limit)
			if !b.hasTokens(executionTime, limit, n) {
				return false
			}
		}
	}

	return true
}

// PeekN returns true if `n` tokens are available for the given input across all limiters,
// but without consuming any tokens.
func (rs *Limiters[TInput, TKey]) PeekN(input TInput, n int64) bool {
	return rs.peekN(input, ntime.Now(), n)
}

// Peek returns true if tokens are available for the given input across all limiters,
// but without consuming any tokens.
func (rs *Limiters[TInput, TKey]) Peek(input TInput) bool {
	return rs.PeekN(input, 1)
}

// PeekWithDetails returns true if tokens are available for the given input across all limiters,
// along with aggregated details optimized for setting response headers.
// This method avoids allocations and is suitable for performance-critical paths.
//
// No tokens are consumed.
func (rs *Limiters[TInput, TKey]) PeekWithDetails(input TInput) (bool, Details[TInput, TKey]) {
	return rs.PeekNWithDetails(input, 1)
}

// PeekNWithDetails returns true if `n` tokens are available for the given input across all limiters,
// along with aggregated details optimized for setting response headers.
// This method avoids allocations and is suitable for performance-critical paths.
//
// No tokens are consumed.
func (rs *Limiters[TInput, TKey]) PeekNWithDetails(input TInput, n int64) (bool, Details[TInput, TKey]) {
	return rs.peekNWithDetails(input, ntime.Now(), n)
}

// peekNWithDetails returns true if `n` tokens are available for the given input across all limiters,
// along with aggregated details optimized for setting response headers.
// This method avoids allocations and is suitable for performance-critical paths.
//
// No tokens are consumed.
//
// Note: This implementation uses read locks (b.mu.RLock()) because the bucket methods
// are designed to handle concurrent access gracefully. While concurrent calls to consumeTokens
// might cause slight inconsistencies between hasTokens and remainingTokens results, this is
// acceptable for peek operations where the main result (allowed) is the primary concern
// and details are treated as predictions rather than guarantees.
func (rs *Limiters[TInput, TKey]) peekNWithDetails(input TInput, executionTime ntime.Time, n int64) (bool, Details[TInput, TKey]) {
	switch len(rs.limiters) {
	case 0:
		return true, Details[TInput, TKey]{
			allowed:         true,
			executionTime:   executionTime.ToSystemTime(),
			tokensRequested: n,
			tokensConsumed:  0,
			tokensRemaining: 0,
			retryAfter:      0,
		}
	case 1:
		return rs.limiters[0].peekNWithDetails(input, executionTime, n)
	}

	// For multiple limiters, we need to check all of them to build the aggregated details
	// Use stack allocation for small numbers
	const maxStackLimiters = 4
	var limitsByLimiter [][]Limit
	if len(rs.limiters) <= maxStackLimiters {
		var stackLimits [maxStackLimiters][]Limit
		limitsByLimiter = stackLimits[:len(rs.limiters)]
	} else {
		limitsByLimiter = make([][]Limit, len(rs.limiters))
	}

	totalLimits := 0
	for i, r := range rs.limiters {
		lims := r.getLimits(input)
		limitsByLimiter[i] = lims
		totalLimits += len(lims)
	}
	if totalLimits == 0 { // all limiters had zero limits
		return true, Details[TInput, TKey]{
			allowed:         true,
			executionTime:   executionTime.ToSystemTime(),
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
	// as we do in allowNWithDetails
	for i, r := range rs.limiters {
		lims := limitsByLimiter[i]
		if len(lims) == 0 {
			continue // No limits for this limiter, so it allows everything
		}

		userKey := r.keyFunc(input)

		// Check each limit for this limiter
		for _, limit := range lims {
			if b, ok := r.buckets.load(userKey, limit); ok {
				b.mu.RLock()
				// First check if allowed (this doesn't modify state)
				allowed := b.hasTokens(executionTime, limit, n)
				allowAll = allowAll && allowed

				// Then get details (these might modify state, but we accept the race condition)
				// since the important thing is the allowed result
				rt := b.remainingTokens(executionTime, limit)
				ra := b.retryAfter(executionTime, limit, n)
				b.mu.RUnlock()

				if remainingTokens == -1 || rt < remainingTokens { // min
					remainingTokens = rt
				}
				if ra > retryAfter { // max
					retryAfter = ra
				}

				continue
			}

			// Use stack-allocated bucket for missing buckets
			b := newBucket(executionTime, limit)
			allowed := b.hasTokens(executionTime, limit, n)
			allowAll = allowAll && allowed

			rt := b.remainingTokens(executionTime, limit)
			ra := b.retryAfter(executionTime, limit, n)

			if remainingTokens == -1 || rt < remainingTokens { // min
				remainingTokens = rt
			}
			if ra > retryAfter { // max
				retryAfter = ra
			}
		}
	}

	if remainingTokens < 0 {
		remainingTokens = 0
	}

	return allowAll, Details[TInput, TKey]{
		allowed:         allowAll,
		executionTime:   executionTime.ToSystemTime(),
		tokensRequested: n,
		tokensConsumed:  0, // Never consume tokens in peek
		tokensRemaining: remainingTokens,
		retryAfter:      retryAfter,
	}
}

// PeekWithDebug returns true if tokens are available for the given input across all limiters,
// along with detailed debugging information about all bucket(s) and the execution time.
// You might use these details for logging, debugging, etc.
//
// Note: This method allocates and may be expensive for performance-critical paths.
// For setting response headers, consider using PeekWithDetails instead.
//
// No tokens are consumed.
func (rs *Limiters[TInput, TKey]) PeekWithDebug(input TInput) (bool, []Debug[TInput, TKey]) {
	return rs.PeekNWithDebug(input, 1)
}

// PeekNWithDebug returns true if `n` tokens are available for the given input across all limiters,
// along with detailed debugging information about all bucket(s) and remaining tokens.
// You might use these details for logging, debugging, etc.
//
// Note: This method allocates and may be expensive for performance-critical paths.
// For setting response headers, consider using PeekNWithDetails instead.
//
// No tokens are consumed.
func (rs *Limiters[TInput, TKey]) PeekNWithDebug(input TInput, n int64) (bool, []Debug[TInput, TKey]) {
	return rs.peekNWithDebug(input, ntime.Now(), n)
}

// peekNWithDebug returns true if `n` tokens are available for the given input across all limiters,
// along with detailed debugging information about all bucket(s) and remaining tokens.
// You might use these details for logging, debugging, etc.
//
// Note: This method allocates and may be expensive for performance-critical paths.
// For setting response headers, consider using PeekNWithDetails instead.
//
// No tokens are consumed.
//
// Note: This implementation uses read locks (b.mu.RLock()) because the bucket methods
// are designed to handle concurrent access gracefully. While concurrent calls to consumeTokens
// might cause slight inconsistencies between hasTokens and remainingTokens results, this is
// acceptable for peek operations where the main result (allowed) is the primary concern
// and details are treated as predictions rather than guarantees.
func (rs *Limiters[TInput, TKey]) peekNWithDebug(input TInput, executionTime ntime.Time, n int64) (bool, []Debug[TInput, TKey]) {
	switch len(rs.limiters) {
	case 0:
		// No limiters, return empty debug info
		return true, []Debug[TInput, TKey]{}
	case 1:
		return rs.limiters[0].peekNWithDebug(input, executionTime, n)
	}

	// For multiple limiters, we need to check all of them to build the debug info
	// Use stack allocation for small numbers
	const maxStackLimiters = 4
	var limitsByLimiter [][]Limit
	if len(rs.limiters) <= maxStackLimiters {
		var stackLimits [maxStackLimiters][]Limit
		limitsByLimiter = stackLimits[:len(rs.limiters)]
	} else {
		limitsByLimiter = make([][]Limit, len(rs.limiters))
	}

	totalLimits := 0
	for i, r := range rs.limiters {
		lims := r.getLimits(input)
		limitsByLimiter[i] = lims
		totalLimits += len(lims)
	}
	if totalLimits == 0 { // all limiters had zero limits
		// No limits, return empty debug info
		return true, []Debug[TInput, TKey]{}
	}

	allowAll := true
	debugs := make([]Debug[TInput, TKey], 0, totalLimits)

	// For peek operation, we can check each bucket individually
	// without needing to collect and lock them all together
	for i, r := range rs.limiters {
		lims := limitsByLimiter[i]
		if len(lims) == 0 {
			continue // No limits for this limiter, so it allows everything
		}

		userKey := r.keyFunc(input)

		// Check each limit for this limiter
		for _, limit := range lims {
			if b, ok := r.buckets.load(userKey, limit); ok {
				b.mu.RLock()

				allow := b.hasTokens(executionTime, limit, n)
				debugs = append(debugs, Debug[TInput, TKey]{
					allowed:         allow,
					executionTime:   executionTime.ToSystemTime(),
					input:           input,
					key:             userKey,
					limit:           limit,
					tokensRequested: n,
					tokensConsumed:  0, // Never consume tokens in peek
					tokensRemaining: b.remainingTokens(executionTime, limit),
					retryAfter:      b.retryAfter(executionTime, limit, n),
				})

				b.mu.RUnlock()
				allowAll = allowAll && allow

				continue
			}

			// Use stack-allocated bucket for missing buckets
			b := newBucket(executionTime, limit)
			allow := b.hasTokens(executionTime, limit, n)
			debugs = append(debugs, Debug[TInput, TKey]{
				allowed:         allow,
				input:           input,
				key:             userKey,
				executionTime:   executionTime.ToSystemTime(),
				limit:           limit,
				tokensRequested: n,
				tokensConsumed:  0,
				tokensRemaining: b.remainingTokens(executionTime, limit),
				retryAfter:      b.retryAfter(executionTime, limit, n),
			})
			allowAll = allowAll && allow
		}
	}

	return allowAll, debugs
}
