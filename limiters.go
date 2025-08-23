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

// GC deletes buckets that are full, i.e, buckets for which enough
// time has passed that they are no longer relevant. A full bucket
// and a non-existent bucket have the same semantics.
//
// Without GC, buckets (memory) will grow unbounded.
//
// This can be a moderately expensive operation, depending
// on the number of buckets. If you want a cheaper operation,
// see [Clear].
func (r *Limiters[TInput, TKey]) GC() (deleted int64) {
	for _, limiter := range r.limiters {
		deleted += limiter.GC()
	}
	return deleted
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
func (r *Limiters[TInput, TKey]) Clear() {
	for _, limiter := range r.limiters {
		limiter.Clear()
	}
}
