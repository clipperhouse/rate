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
	if len(rs.limiters) == 0 {
		return true
	}

	// Optimization: use stack allocation for small number of limiters
	const maxStackLimiters = 4
	var limitsByLimiter [][]Limit
	if len(rs.limiters) <= maxStackLimiters {
		var stackLimits [maxStackLimiters][]Limit
		limitsByLimiter = stackLimits[:len(rs.limiters)]
	} else {
		limitsByLimiter = make([][]Limit, len(rs.limiters))
	}

	count := 0
	for i, r := range rs.limiters {
		lims := r.getLimits(input)
		limitsByLimiter[i] = lims
		count += len(lims)
	}
	if count == 0 {
		return true
	}

	// Optimization: use stack allocation for small number of limits
	const maxStackLimits = 6
	var limits []Limit
	if count <= maxStackLimits {
		var stackLimits [maxStackLimits]Limit
		limits = stackLimits[:0]
	} else {
		limits = make([]Limit, 0, count)
	}
	for _, lims := range limitsByLimiter {
		limits = append(limits, lims...)
	}

	// Collect buckets in same order as limits
	// Optimization: use stack allocation for small number of limits
	var buckets []*bucket
	const maxStackBuckets = 6
	if count <= maxStackBuckets {
		var stackBuckets [maxStackBuckets]*bucket
		buckets = stackBuckets[:0]
	} else {
		buckets = make([]*bucket, 0, count)
	}

	for i, r := range rs.limiters {
		lims := limitsByLimiter[i]
		if len(lims) == 0 {
			continue
		}
		userKey := r.keyer(input)
		for _, limit := range lims {
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

	for i, b := range buckets {
		if !b.hasTokens(executionTime, limits[i], n) {
			return false
		}
	}

	// All are allowed, consume tokens
	for i, b := range buckets {
		b.consumeTokens(executionTime, limits[i], n)
	}
	return true
}
