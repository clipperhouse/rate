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
func (sm *syncMap[K, V]) loadOrCreate(key K, getter func() V) V {
	loaded, ok := sm.m.Load(key)
	if ok {
		return loaded.(V)
	}
	return getter()
}

// loadOrStore returns the existing value for the key if present.
// Otherwise, it calls the factory function to create a new value, stores it, and returns it.
// This avoids creating the value unless it's actually needed.
func (sm *syncMap[K, V]) loadOrStore(key K, getter func() V) V {
	if loaded, ok := sm.m.Load(key); ok {
		return loaded.(V)
	}
	// Only create the value if we didn't find an existing one
	value := getter()
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

// delete removes a key from the map
func (sm *syncMap[K, V]) delete(key K) {
	sm.m.Delete(key)
}
