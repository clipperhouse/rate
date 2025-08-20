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
	return r.waitN(ctx, input, ntime.Now(), 1)
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
	return r.waitN(ctx, input, ntime.Now(), n)
}

func (r *Limiter[TInput, TKey]) waitN(ctx context.Context, input TInput, executionTime ntime.Time, n int64) bool {
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
	startTime ntime.Time,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	return r.waitNWithCancellation(input, startTime, 1, deadline, done)
}

// waitWithCancellation is a more testable version of wait that accepts
// deadline and done functions instead of a context, allowing for deterministic testing.
func (r *Limiter[TInput, TKey]) waitNWithCancellation(
	input TInput,
	startTime ntime.Time,
	n int64,
	deadline func() (time.Time, bool),
	done func() <-chan struct{},
) bool {
	// currentTime is an approximation of the real clock moving forward
	// it's imprecise because it depends on time.After below.
	// For testing purposes, we want startTime (execution time) to
	// be a parameter.
	currentTime := startTime

	for {
		allow, details := r.allowNWithDetails(input, currentTime, n)
		if allow {
			return true
		}

		retryAfter := details.RetryAfter()

		// If we can't possibly get a token, fail fast
		if deadline, ok := deadline(); ok {
			// ctx deadline uses time.Time, not ntime.Time,
			// so use a duration for the comparison.
			// This might not be robust to clock skew.
			d := deadline.Sub(currentTime.ToTime())
			if d < retryAfter {
				return false
			}
		}

		select {
		case <-done():
			return false
		case <-time.After(retryAfter):
			currentTime = currentTime.Add(retryAfter)
		}
	}
}
