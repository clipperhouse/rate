package rate

// Limiter is a rate limiter that can be used to limit the rate of requests to a given key.
type Limiter[TInput any, TKey comparable] struct {
	keyFunc    KeyFunc[TInput, TKey]
	limits     []Limit
	limitFuncs []LimitFunc[TInput]
	buckets    bucketMap[TKey]
	waiters    syncMap[TKey, *waiter]
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
