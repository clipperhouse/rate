package rate

import "github.com/clipperhouse/ntime"

func (rs *Limiters[TInput, TKey]) Allow(input TInput) bool {
	return rs.AllowN(input, 1)
}

func (rs *Limiters[TInput, TKey]) AllowN(input TInput, n int64) bool {
	return rs.allowN(input, ntime.Now(), n)
}

// allowN returns true if at least `n` tokens are available for the given input,
// across all Limiters.
//
// If true, it will consume `n` tokens. If false, no token will be consumed.
//
// allowN will return true only if all Limiters allow the request,
// and `n` tokens will be consumed against each limit. If any limit
// would be exceeded, no token will be consumed against from any Limiter.
func (rs *Limiters[TInput, TKey]) allowN(input TInput, executionTime ntime.Time, n int64) bool {
	switch len(rs.limiters) {
	case 0:
		return true
	case 1:
		return rs.limiters[0].allowN(input, executionTime, n)
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
		userKey := r.keyFunc(input)
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

func (rs *Limiters[TInput, TKey]) AllowWithDetails(input TInput) (bool, Details[TInput, TKey]) {
	return rs.AllowNWithDetails(input, 1)
}

func (rs *Limiters[TInput, TKey]) AllowNWithDetails(input TInput, n int64) (bool, Details[TInput, TKey]) {
	return rs.allowNWithDetails(input, ntime.Now(), n)
}

func (rs *Limiters[TInput, TKey]) allowNWithDetails(input TInput, executionTime ntime.Time, n int64) (bool, Details[TInput, TKey]) {
	if len(rs.limiters) == 0 { // no limiters -> allow everything
		return true, Details[TInput, TKey]{
			allowed:         true,
			executionTime:   executionTime.ToTime(),
			tokensRequested: n,
			tokensConsumed:  0,
			tokensRemaining: 0,
			retryAfter:      0,
		}
	}

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
			executionTime:   executionTime.ToTime(),
			tokensRequested: n,
			tokensConsumed:  0,
			tokensRemaining: 0,
			retryAfter:      0,
		}
	}

	const maxStackLimits = 6
	var limits []Limit
	if totalLimits <= maxStackLimits {
		var stackLimits [maxStackLimits]Limit
		limits = stackLimits[:0]
	} else {
		limits = make([]Limit, 0, totalLimits)
	}
	for _, lims := range limitsByLimiter {
		limits = append(limits, lims...)
	}

	const maxStackBuckets = 6
	var buckets []*bucket
	if totalLimits <= maxStackBuckets {
		var stackBuckets [maxStackBuckets]*bucket
		buckets = stackBuckets[:0]
	} else {
		buckets = make([]*bucket, 0, totalLimits)
	}

	for i, r := range rs.limiters {
		lims := limitsByLimiter[i]
		if len(lims) == 0 {
			continue
		}
		userKey := r.keyFunc(input)
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

	allowAll := buckets[0].hasTokens(executionTime, limits[0], n)
	remainingTokens := buckets[0].remainingTokens(executionTime, limits[0])
	retryAfter := buckets[0].retryAfter(executionTime, limits[0], n)
	for i := 1; i < len(buckets); i++ {
		b := buckets[i]
		limit := limits[i]
		if !b.hasTokens(executionTime, limit, n) {
			allowAll = false
		}
		rt := b.remainingTokens(executionTime, limit)
		if rt < remainingTokens {
			remainingTokens = rt
		}
		ra := b.retryAfter(executionTime, limit, n)
		if ra > retryAfter {
			retryAfter = ra
		}
	}

	if remainingTokens < 0 {
		remainingTokens = 0
	}

	consumed := int64(0)
	if allowAll {
		for i, b := range buckets {
			b.consumeTokens(executionTime, limits[i], n)
			rt := b.remainingTokens(executionTime, limits[i])
			if rt < remainingTokens {
				remainingTokens = rt
			}
		}
		consumed = n
		if remainingTokens < 0 {
			remainingTokens = 0
		}
	}

	return allowAll, Details[TInput, TKey]{
		allowed:         allowAll,
		executionTime:   executionTime.ToTime(),
		tokensRequested: n,
		tokensConsumed:  consumed,
		tokensRemaining: remainingTokens,
		retryAfter:      retryAfter,
	}
}

func (rs *Limiters[TInput, TKey]) AllowWithDebug(input TInput) (bool, []Debug[TInput, TKey]) {
	return rs.AllowNWithDebug(input, 1)
}

func (rs *Limiters[TInput, TKey]) AllowNWithDebug(input TInput, n int64) (bool, []Debug[TInput, TKey]) {
	return rs.allowNWithDebug(input, ntime.Now(), n)
}

func (rs *Limiters[TInput, TKey]) allowNWithDebug(input TInput, executionTime ntime.Time, n int64) (bool, []Debug[TInput, TKey]) {
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

	const maxStackLimits = 6
	var limits []Limit
	if totalLimits <= maxStackLimits {
		var stackLimits [maxStackLimits]Limit
		limits = stackLimits[:0]
	} else {
		limits = make([]Limit, 0, totalLimits)
	}
	for _, lims := range limitsByLimiter {
		limits = append(limits, lims...)
	}

	const maxStackBuckets = 6
	var buckets []*bucket
	if totalLimits <= maxStackBuckets {
		var stackBuckets [maxStackBuckets]*bucket
		buckets = stackBuckets[:0]
	} else {
		buckets = make([]*bucket, 0, totalLimits)
	}

	for i, r := range rs.limiters {
		lims := limitsByLimiter[i]
		if len(lims) == 0 {
			continue
		}
		userKey := r.keyFunc(input)
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

	debugs := make([]Debug[TInput, TKey], len(buckets))
	allowAll := true

	// Build debugs by iterating through buckets and matching with limits
	bucketIndex := 0
	for i, r := range rs.limiters {
		lims := limitsByLimiter[i]
		if len(lims) == 0 {
			continue
		}
		userKey := r.keyFunc(input)
		for _, limit := range lims {
			b := buckets[bucketIndex]
			allow := b.hasTokens(executionTime, limit, n)
			allowAll = allowAll && allow

			debugs[bucketIndex] = Debug[TInput, TKey]{
				allowed:         allow,
				executionTime:   executionTime.ToTime(),
				input:           input,
				key:             userKey,
				limit:           limit,
				tokensRequested: n,
			}
			bucketIndex++
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
		debugs[i].retryAfter = b.retryAfter(executionTime, limit, n)
	}

	return false, debugs
}
