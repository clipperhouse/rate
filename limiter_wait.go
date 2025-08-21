package rate

import (
	"context"
	"time"

	"github.com/clipperhouse/ntime"
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
// Wait makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiter[TInput, TKey]) Wait(ctx context.Context, input TInput) bool {
	return r.WaitN(ctx, input, 1)
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
// WaitN makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiter[TInput, TKey]) WaitN(ctx context.Context, input TInput, n int64) bool {
	allow, _ := r.waitNWithDetails(
		input,
		ntime.Now(),
		n,
		ctx.Deadline,
		ctx.Done,
	)
	return allow
}

// WaitWithDetails will poll [Allow] for a period of time,
// until it is cancelled by the passed context. It has the
// effect of adding latency to requests instead of refusing
// them immediately. Consider it graceful degradation.
//
// WaitWithDetails will return true if a token becomes available prior to
// the context cancellation, and will consume a token. It will
// return false if not, and therefore not consume a token. It will
// also return the details of the request.
//
// Take care to create an appropriate context. You almost certainly
// want [context.WithTimeout] or [context.WithDeadline].
//
// You should be conservative, as WaitWithDetails will introduce
// backpressure on your upstream systems -- connections
// may be held open longer, requests may queue in memory.
//
// A good starting place will be to timeout after waiting
// for one token. For example:
//
//	ctx := context.WithTimeout(ctx, limit.DurationPerToken())
//
// WaitWithDetails makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiter[TInput, TKey]) WaitWithDetails(ctx context.Context, input TInput) (bool, Details[TInput, TKey]) {
	return r.WaitNWithDetails(ctx, input, 1)
}

// WaitNWithDetails will poll [Limiter.AllowN] for a period of time,
// until it is cancelled by the passed context. It has the
// effect of adding latency to requests instead of refusing
// them immediately. Consider it graceful degradation.
//
// WaitNWithDetails will return true if `n` tokens become available prior to
// the context cancellation, and will consume `n` tokens. If not,
// it will return false, and therefore consume no tokens. It will
// also return the details of the request.
//
// Take care to create an appropriate context. You almost certainly
// want [context.WithTimeout] or [context.WithDeadline].
//
// You should be conservative, as WaitNWithDetails will introduce
// backpressure on your upstream systems -- connections
// may be held open longer, requests may queue in memory.
//
// A good starting place will be to timeout after waiting
// for one token. For example:
//
//	ctx := context.WithTimeout(ctx, limit.DurationPerToken())
//
// WaitNWithDetails makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiter[TInput, TKey]) WaitNWithDetails(ctx context.Context, input TInput, n int64) (bool, Details[TInput, TKey]) {
	return r.waitNWithDetails(
		input,
		ntime.Now(),
		n,
		ctx.Deadline,
		ctx.Done,
	)
}

func (r *Limiter[TInput, TKey]) waitWithCancellation(
	input TInput,
	startTime ntime.Time,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	allow, _ := r.waitNWithDetails(input, startTime, 1, deadline, done)
	return allow
}

func (r *Limiter[TInput, TKey]) waitNWithCancellation(
	input TInput,
	startTime ntime.Time,
	n int64,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	allow, _ := r.waitNWithDetails(input, startTime, n, deadline, done)
	return allow
}

// waitWithCancellation is a more testable version of wait that accepts
// deadline and done functions instead of a context, allowing for deterministic testing.
func (r *Limiter[TInput, TKey]) waitNWithDetails(
	input TInput,
	startTime ntime.Time,
	n int64,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) (bool, Details[TInput, TKey]) {
	// currentTime is an approximation of the real clock moving forward.
	// It's imprecise because it depends on time.After below.
	// For testing purposes, we want startTime (execution time) to
	// be a parameter.
	currentTime := startTime

	for {
		allow, details := r.allowNWithDetails(input, currentTime, n)
		if allow {
			return allow, details
		}

		retryAfter := details.RetryAfter()

		// If we can't possibly get a token, fail fast
		if deadline, ok := deadline(); ok {
			// ctx deadline uses time.Time, not ntime.Time,
			// so use a duration for the comparison.
			// This might not be robust to clock skew.
			d := deadline.Sub(currentTime.ToTime())
			if d < retryAfter {
				return false, details
			}
		}

		select {
		case <-done():
			// Do one more check on cancellation,
			// to have accurate retryAfter.
			return r.allowNWithDetails(input, currentTime, n)
		case <-time.After(retryAfter):
			currentTime = currentTime.Add(retryAfter)
		}
	}
}
