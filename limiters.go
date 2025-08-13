package rate

// Limiters is a collection of [Limiter].
type Limiters[TInput any, TKey comparable] struct {
	limiters []*Limiter[TInput, TKey]
}

// Combine combines multiple [Limiter] into a single [Limiters],
// which can be treated like a single with Allow, etc.
func Combine[TInput any, TKey comparable](limiters ...*Limiter[TInput, TKey]) *Limiters[TInput, TKey] {
	return &Limiters[TInput, TKey]{
		limiters: limiters,
	}
}
