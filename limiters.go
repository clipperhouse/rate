package rate

// Limiters is a collection of [Limiter].
type Limiters[TInput any, TKey comparable] struct {
	limiters []*Limiter[TInput, TKey]
}

// NewLimiters combines multiple [Limiter] instances, allowing for more complex rate limiting scenarios.
func NewLimiters[TInput any, TKey comparable](limiters ...*Limiter[TInput, TKey]) *Limiters[TInput, TKey] {
	return &Limiters[TInput, TKey]{
		limiters: limiters,
	}
}
