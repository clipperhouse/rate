package rate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Wait will poll [Allow] for a period of time,
// until it is cancelled by the passed context. It has the
// effect of adding latency to requests instead of refusing
// them immediately. Consider it graceful degradation.
//
// Wait will return true if a token becomes available prior to
// the context cancellation, and will consume a token. It will
// return false if not, and therefore not consume a token.
//
// Take care to create an appropriate context. You almost certainly
// want [context.WithTimeout] or [context.WithDeadline].
//
// You should be conservative, as Wait will introduce
// backpressure on your upstream systems -- connections
// may be held open longer, requests may queue in memory.
//
// A good starting place will be to timeout after waiting
// for one token. For example:
//
//	ctx := context.WithTimeout(ctx, limit.DurationPerToken())
//
// Wait offers best-effort FIFO ordering of requests. Under sustained
// contention (when multiple requests wait longer than ~1ms), the Go runtime's
// mutex starvation mode ensures strict FIFO ordering. Under light load,
// ordering may be less strict but performance is optimized.
func (r *Limiter[TInput, TKey]) Wait(ctx context.Context, input TInput) bool {
	return r.waitN(ctx, input, time.Now(), 1)
}

// WaitN will poll [Limiter.AllowN] for a period of time,
// until it is cancelled by the passed context. It has the
// effect of adding latency to requests instead of refusing
// them immediately. Consider it graceful degradation.
//
// WaitN will return true if `n` tokens become available prior to
// the context cancellation, and will consume `n` tokens. If not,
// it will return false, and therefore consume no tokens.
//
// Take care to create an appropriate context. You almost certainly
// want [context.WithTimeout] or [context.WithDeadline].
//
// You should be conservative, as Wait will introduce
// backpressure on your upstream systems -- connections
// may be held open longer, requests may queue in memory.
//
// A good starting place will be to timeout after waiting
// for one token. For example:
//
//	ctx := context.WithTimeout(ctx, limit.DurationPerToken())
//
// WaitN offers best-effort FIFO ordering of requests. Under sustained
// contention (when multiple requests wait longer than ~1ms), the Go runtime's
// mutex starvation mode ensures strict FIFO ordering. Under light load,
// ordering may be less strict but performance is optimized.
func (r *Limiter[TInput, TKey]) WaitN(ctx context.Context, input TInput, n int64) bool {
	return r.waitN(ctx, input, time.Now(), n)
}

func (r *Limiter[TInput, TKey]) waitN(ctx context.Context, input TInput, executionTime time.Time, n int64) bool {
	return r.waitNWithCancellation(
		input,
		executionTime,
		n,
		ctx.Deadline,
		ctx.Done,
	)
}

func (r *Limiter[TInput, TKey]) waitWithCancellation(
	input TInput,
	startTime time.Time,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	return r.waitNWithCancellation(input, startTime, 1, deadline, done)
}

// waitWithCancellation is a more testable version of wait that accepts
// deadline and done functions instead of a context, allowing for deterministic testing.
func (r *Limiter[TInput, TKey]) waitNWithCancellation(
	input TInput,
	startTime time.Time,
	n int64,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	if r.allowN(input, startTime, n) {
		return true
	}

	// currentTime is an approximation of the real clock moving forward
	// it's imprecise because it depends on time.After below.
	// For testing purposes, I want startTime (execution time) to
	// be a parameter. The alternative is calling time.Now().
	currentTime := startTime

	userKey := r.keyer(input)
	waiter := r.getWaiter(userKey)

	// Ensure cleanup happens when this waiter exits
	defer func() {
		// If no more waiters for this key, remove the entry to prevent memory leak,
		// reference-counted
		if waiter.decrement() == 0 {
			r.waiters.delete(userKey)
		}
	}()

	for {
		// The goroutine at the front of the queue gets to try for a token first.
		waiter.mu.Lock()
		allow, details := r.allowNWithDetails(input, currentTime, n)
		waiter.mu.Unlock()
		if allow {
			return true
		}

		wait := max(details.RetryAfter(), 0)

		// if we can't possibly get a token, fail fast
		if deadline, ok := deadline(); ok {
			if deadline.Before(currentTime.Add(wait)) {
				return false
			}
		}

		select {
		case <-done():
			return false
		case <-time.After(wait):
			currentTime = currentTime.Add(wait)
		}
	}
}

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

func getWaiter() *waiter {
	return &waiter{}
}

// getWaiter atomically gets or creates a waiter entry and increments its reference count
func (r *Limiter[TInput, TKey]) getWaiter(key TKey) *waiter {
	waiter := r.waiters.loadOrStore(key, getWaiter)
	waiter.increment()
	return waiter
}
