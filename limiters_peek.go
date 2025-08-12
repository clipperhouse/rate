package rate

import "time"

// peekN returns true if tokens are available for the given input across all limiters,
// but without consuming any tokens. This method efficiently checks multiple limiters
// by testing each one individually and returning early if any limit is exceeded.
func (rs *Limiters[TInput, TKey]) peekN(input TInput, executionTime time.Time, n int64) bool {
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
			b := bucket{
				time: executionTime.Add(-limit.period),
			}
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
	return rs.peekN(input, time.Now(), n)
}

// Peek returns true if tokens are available for the given input across all limiters,
// but without consuming any tokens.
func (rs *Limiters[TInput, TKey]) Peek(input TInput) bool {
	return rs.PeekN(input, 1)
}
