package rate

import "sync"

// syncMap is a typed wrapper around sync.Map for our specific use case
type syncMap[K comparable, V any] struct {
	m sync.Map
}

// loadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (sm *syncMap[K, V]) loadOrStore(key K, value V) V {
	actual, _ := sm.m.LoadOrStore(key, value)
	return actual.(V)
}
