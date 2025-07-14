package rate

import "sync"

// syncMap is a typed wrapper around sync.Map for our specific use case
type syncMap[K comparable, V any] struct {
	m sync.Map
}

// loadOrReturn returns the value stored in the map for a key,
// or the passed default value if the key does not exist.
// Unlike loadOrStore, this does not store the default value in the map,
// making it suitable for read-only operations like Peek.
func (sm *syncMap[K, V]) loadOrReturn(key K, value V) V {
	if value, ok := sm.m.Load(key); ok {
		return value.(V)
	}
	return value
}

// loadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (sm *syncMap[K, V]) loadOrStore(key K, value V) V {
	actual, _ := sm.m.LoadOrStore(key, value)
	return actual.(V)
}
