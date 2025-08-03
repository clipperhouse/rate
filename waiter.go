package rate

import (
	"sync"
	"sync/atomic"
)

// waiter represents a reservation queue for a specific key with reference counting
type waiter struct {
	mu    sync.Mutex
	count int64 // number of active waiters for this key
}

// increment atomically increments the waiter count and returns the new count
func (w *waiter) increment() int64 {
	return atomic.AddInt64(&w.count, 1)
}

// decrement atomically decrements the waiter count and returns the new count
func (w *waiter) decrement() int64 {
	return atomic.AddInt64(&w.count, -1)
}

func newWaiter() *waiter {
	return &waiter{}
}
