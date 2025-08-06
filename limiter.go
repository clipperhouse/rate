package rate

import (
	"time"
)

// Limiter is a rate limiter that can be used to limit the rate of requests to a given key.
type Limiter[TInput any, TKey comparable] struct {
	keyer      Keyer[TInput, TKey]
	limits     []Limit
	limitFuncs []LimitFunc[TInput]
	buckets    bucketMap[TKey]
	waiters    syncMap[TKey, *waiter]
}

// Keyer is a function that takes an input and returns a bucket key.
type Keyer[TInput any, TKey comparable] func(input TInput) TKey

// NewLimiter creates a new rate limiter
func NewLimiter[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limits ...Limit) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer:  keyer,
		limits: limits,
	}
}

// NewLimiterFunc creates a new rate limiter with a dynamic limit function. Use this if you
// wish to apply a different limit based on the input, for example by URL path. The LimitFunc
// takes the same input type as the Keyer function.
func NewLimiterFunc[TInput any, TKey comparable](keyer Keyer[TInput, TKey], limitFuncs ...LimitFunc[TInput]) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyer:      keyer,
		limitFuncs: limitFuncs,
	}
}

func (r *Limiter[TInput, TKey]) getBucketSpecs(input TInput, userKey TKey) []bucketSpec[TKey] {
	// limits and limitFuncs are mutually exclusive.
	if len(r.limitFuncs) > 0 {
		// Fast path for single limit function - avoid slice allocation
		if len(r.limitFuncs) == 1 {
			spec := bucketSpec[TKey]{
				limit:   r.limitFuncs[0](input),
				userKey: userKey,
			}
			return []bucketSpec[TKey]{spec}
		}

		specs := make([]bucketSpec[TKey], len(r.limitFuncs))
		for i, limitFunc := range r.limitFuncs {
			specs[i] = bucketSpec[TKey]{
				limit:   limitFunc(input),
				userKey: userKey,
			}
		}
		return specs
	}

	if len(r.limits) > 0 {
		// Fast path for single limit - avoid slice allocation
		if len(r.limits) == 1 {
			spec := bucketSpec[TKey]{
				limit:   r.limits[0],
				userKey: userKey,
			}
			return []bucketSpec[TKey]{spec}
		}

		specs := make([]bucketSpec[TKey], len(r.limits))
		for i, limit := range r.limits {
			specs[i] = bucketSpec[TKey]{
				limit:   limit,
				userKey: userKey,
			}
		}
		return specs
	}

	return nil
}

// getBucketsAndLimits retrieves the buckets and limits for the given input and execution time.
// It returns "parallel" slices of buckets and limits; the i'th element in each slice corresponds
// to the same limit/bucket pair.
func (r *Limiter[TInput, TKey]) getBucketsAndLimits(input TInput, userKey TKey, executionTime time.Time, persist bool) ([]*bucket, []Limit) {
	// Fast path for single static limit - avoid slice allocations
	if len(r.limits) == 1 && len(r.limitFuncs) == 0 {
		limit := r.limits[0]
		var b *bucket
		if persist {
			b = r.buckets.loadOrStore(userKey, executionTime, limit)
		} else {
			b = r.buckets.loadOrGet(userKey, executionTime, limit)
		}

		// Return slices with single elements - this still allocates but less than before
		return []*bucket{b}, []Limit{r.limits[0]}
	}

	specs := r.getBucketSpecs(input, userKey)
	buckets := make([]*bucket, len(specs))
	limits := make([]Limit, len(specs))

	// buckets and limits must be the same length and ordering,
	// so the right limit is applied to the right bucket.

	for i, spec := range specs {
		var b *bucket
		if persist {
			b = r.buckets.loadOrStore(userKey, executionTime, spec.limit)
		} else {
			b = r.buckets.loadOrGet(userKey, executionTime, spec.limit)
		}
		buckets[i] = b
		limits[i] = spec.limit
	}

	return buckets, limits
}

func (r *Limiter[TInput, TKey]) getLimits(input TInput) []Limit {
	limits := r.limits

	// limits and limitFuncs are mutually exclusive.
	if len(r.limitFuncs) > 0 {
		limits = make([]Limit, 0, len(r.limitFuncs))
		for _, limitFunc := range r.limitFuncs {
			limits = append(limits, limitFunc(input))
		}
	}
	return limits
}
