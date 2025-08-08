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
	// Gather per-limiter limits (avoid copying them into a single slice)
	if len(rs.limiters) == 0 {
		return true
	}

	// Small-stack optimization for per-limiter limits slice to avoid heap allocs in common cases
	const maxStackLimiters = 4
	var limitsByLimiter []([]Limit)
	if len(rs.limiters) <= maxStackLimiters {
		var stackPerLimiter [maxStackLimiters][]Limit
		limitsByLimiter = stackPerLimiter[:len(rs.limiters)]
	} else {
		limitsByLimiter = make([][]Limit, len(rs.limiters))
	}
	totalLimits := 0
	for i, r := range rs.limiters {
		limits := r.getLimits(input)
		limitsByLimiter[i] = limits
		totalLimits += len(limits)
	}
	if totalLimits == 0 {
		return true // no limits overall
	}

	// Collect buckets in a flat slice (retain previous locking order) with stack opt
	var buckets []*bucket
	const maxStackBuckets = 6
	if totalLimits <= maxStackBuckets {
		var stackBuckets [maxStackBuckets]*bucket
		buckets = stackBuckets[:0]
	} else {
		buckets = make([]*bucket, 0, totalLimits)
	}

	for i, r := range rs.limiters {
		limits := limitsByLimiter[i]
		if len(limits) == 0 {
			continue
		}
		userKey := r.keyer(input)
		for _, limit := range limits {
			b := r.buckets.loadOrStore(userKey, executionTime, limit)
			buckets = append(buckets, b)
			b.mu.Lock()
		}
	}
	defer func() {
		for _, b := range buckets {
			b.mu.Unlock()
		}
	}()

	// Check all buckets using two-level indexing over perLimiterLimits
	allowAll := true
	flat := 0
	outerBreak := false
	for _, limits := range limitsByLimiter {
		if len(limits) == 0 {
			continue
		}
		for li := range limits {
			b := buckets[flat]
			limit := limits[li]
			if !b.hasTokens(executionTime, limit, n) {
				allowAll = false
				outerBreak = true
				break
			}
			flat++
		}
		if outerBreak {
			break
		}
	}

	if allowAll {
		flat = 0
		for _, limits := range limitsByLimiter {
			if len(limits) == 0 {
				continue
			}
			for li := range limits {
				b := buckets[flat]
				limit := limits[li]
				b.consumeTokens(executionTime, limit, n)
				flat++
			}
		}
	}
	return allowAll
}
