package rate

import (
	"sync"
)

// syncMap is a typed wrapper around sync.Map for our specific use case
type syncMap[K comparable, V any] struct {
	m sync.Map
}

// loadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (sm *syncMap[K, V]) loadOrReturn(key K, value V) V {
	loaded, ok := sm.m.Load(key)
	if ok {
		return loaded.(V)
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

func (sm *syncMap[K, V]) count() int {
	count := 0
	sm.m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
