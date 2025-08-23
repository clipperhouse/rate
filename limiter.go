package rate

import "github.com/clipperhouse/ntime"

// Limiter is a rate limiter that can be used to limit the rate of requests to a given key.
type Limiter[TInput any, TKey comparable] struct {
	keyFunc    KeyFunc[TInput, TKey]
	limits     []Limit
	limitFuncs []LimitFunc[TInput]
	buckets    bucketMap[TKey]
}

// KeyFunc is a function that takes an input and returns a bucket key.
type KeyFunc[TInput any, TKey comparable] func(input TInput) TKey

// NewLimiter creates a new rate limiter
func NewLimiter[TInput any, TKey comparable](keyFunc KeyFunc[TInput, TKey], limits ...Limit) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyFunc: keyFunc,
		limits:  limits,
	}
}

// NewLimiterFunc creates a new rate limiter with a dynamic limit function. Use this if you
// wish to apply a different limit based on the input, for example by URL path. The LimitFunc
// takes the same input type as the Keyer function.
func NewLimiterFunc[TInput any, TKey comparable](keyFunc KeyFunc[TInput, TKey], limitFuncs ...LimitFunc[TInput]) *Limiter[TInput, TKey] {
	return &Limiter[TInput, TKey]{
		keyFunc:    keyFunc,
		limitFuncs: limitFuncs,
	}
}

func (r *Limiter[TInput, TKey]) getLimits(input TInput) []Limit {
	if len(r.limits) > 0 {
		return r.limits
	}

	// limits and limitFuncs are mutually exclusive.
	limits := make([]Limit, len(r.limitFuncs))
	for i, limitFunc := range r.limitFuncs {
		limits[i] = limitFunc(input)
	}
	return limits
}

// GC deletes buckets that are full, i.e, buckets for which enough
// time has passed that they are no longer relevant. A full bucket
// and a non-existent bucket have the same semantics.
//
// Without GC, buckets (memory) will grow unbounded.
//
// This can be a moderately expensive operation, depending
// on the number of buckets. If you want a cheaper operation,
// see [Clear].
func (r *Limiter[TInput, TKey]) GC() (deleted int64) {
	return r.buckets.gc(ntime.Now)
}

// Clear deletes all buckets. This is semantically
// equivalent to refilling all buckets.
//
// You would use this method for garbage collection
// purposes, as the limiter's memory will grow unbounded
// otherwise.
//
// See also the [GC] method, which is more selective, and
// only deletes buckets that are no longer meaningful.
func (r *Limiter[TInput, TKey]) Clear() {
	r.buckets.m.Clear()
}
