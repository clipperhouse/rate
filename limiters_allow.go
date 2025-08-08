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

	// First pass: gather slices & count.
	totalLimits := 0
	for i, r := range rs.limiters {
		ls := r.getLimits(input)
		limitsByLimiter[i] = ls
		totalLimits += len(ls)
	}
	if totalLimits == 0 {
		return true
	}

	// Optimization: use stack allocation for small number of limits
	const maxStackLimits = 6
	var allLimits []Limit
	if totalLimits <= maxStackLimits {
		var stackLimits [maxStackLimits]Limit
		allLimits = stackLimits[:0]
	} else {
		allLimits = make([]Limit, 0, totalLimits)
	}
	for _, ls := range limitsByLimiter {
		if len(ls) == 0 {
			continue
		}
		allLimits = append(allLimits, ls...)
	}

	// Collect buckets in same order as allLimits
	// Optimization: use stack allocation for small number of limits
	var buckets []*bucket
	const maxStackBuckets = 6
	if totalLimits <= maxStackBuckets {
		var stackBuckets [maxStackBuckets]*bucket
		buckets = stackBuckets[:0]
	} else {
		buckets = make([]*bucket, 0, totalLimits)
	}

	for i, r := range rs.limiters {
		ls := limitsByLimiter[i]
		if len(ls) == 0 {
			continue
		}
		userKey := r.keyer(input)
		for _, limit := range ls {
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
		if !b.hasTokens(executionTime, allLimits[i], n) {
			return false
		}
	}

	// All are allowed, consume tokens
	for i, b := range buckets {
		b.consumeTokens(executionTime, allLimits[i], n)
	}
	return true
}
