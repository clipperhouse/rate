package rate

import "time"

// allowN returns true if at least `n` tokens are available for the given input,
// across all Limiters.
//
// If true, it will consume `n` tokens. If false, no token will be consumed.
//
// allowN will return true only if all Limiters allow the request,
// and `n` tokens will be consumed against each limit. If any limit
// would be exceeded, no token will be consumed against from any Limiter.
func (rs *Limiters[TInput, TKey]) allowN(input TInput, executionTime time.Time, n int64) bool {
	// Count the allLimits for allocating buckets
	var allLimits []Limit
	for _, r := range rs.limiters {
		l := r.getLimits(input)
		allLimits = append(allLimits, l...)
	}
	if len(allLimits) == 0 {
		// No limits defined, allow everything
		return true
	}

	// Collect the buckets
	var buckets []*bucket

	// Optimization: use a stack allocation for small numbers of limits,
	// should be the common case
	const maxStackBuckets = 6
	if len(allLimits) <= maxStackBuckets {
		var stackBuckets [maxStackBuckets]*bucket
		buckets = stackBuckets[:0]
	} else {
		// Fall back to heap for larger cases
		buckets = make([]*bucket, 0, len(allLimits))
	}

	for _, r := range rs.limiters {
		// Just the limits for this limiter
		limits := r.getLimits(input)
		if len(limits) == 0 {
			continue
		}

		userKey := r.keyer(input)

		// We need to collect all buckets and lock them together
		for _, limit := range limits {
			b := r.buckets.loadOrStore(userKey, executionTime, limit)
			buckets = append(buckets, b)
			b.mu.Lock()
		}
	}
	// ...and defer unlock
	defer func() {
		for _, b := range buckets {
			b.mu.Unlock()
		}
	}()

	// Check if all buckets allow
	allowAll := true
	for i, b := range buckets {
		limit := allLimits[i]
		if !b.hasTokens(executionTime, limit, n) {
			allowAll = false
			break
		}
	}

	// Consume tokens only when all buckets allow
	if allowAll {
		for i := range buckets {
			b := buckets[i]
			limit := allLimits[i]
			b.consumeTokens(executionTime, limit, n)
		}
	}
	return allowAll
}
