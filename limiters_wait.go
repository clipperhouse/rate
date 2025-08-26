package rate

import (
	"context"
	"time"

	"github.com/clipperhouse/ntime"
)

// Wait will poll [Limiters.Allow] for a period of time,
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
// The returned error will be non-nil if the context is cancelled.
//
// Wait makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiters[TInput, TKey]) Wait(ctx context.Context, input TInput) (bool, error) {
	return r.WaitN(ctx, input, 1)
}

// WaitN will poll [Limiters.AllowN] for a period of time,
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
// The returned error will be non-nil if the context is cancelled.
//
// WaitN makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiters[TInput, TKey]) WaitN(ctx context.Context, input TInput, n int64) (bool, error) {
	allow, _, err := r.WaitNWithDetails(ctx, input, n)
	return allow, err
}

// WaitWithDetails will poll [Limiters.Allow] for a period of time,
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
// The returned error will be non-nil if the context is cancelled.
//
// WaitWithDetails makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiters[TInput, TKey]) WaitWithDetails(ctx context.Context, input TInput) (bool, Details[TInput, TKey], error) {
	return r.WaitNWithDetails(ctx, input, 1)
}

// WaitNWithDetails will poll [Limiters.AllowN] for a period of time,
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
// You should be conservative, as WaitWithDetails will introduce
// backpressure on your upstream systems -- connections
// may be held open longer, requests may queue in memory.
//
// A good starting place will be to timeout after waiting
// for one token. For example:
//
//	ctx := context.WithTimeout(ctx, limit.DurationPerToken())
//
// The returned error will be non-nil if the context is cancelled.
//
// WaitWithDetails makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiters[TInput, TKey]) WaitNWithDetails(ctx context.Context, input TInput, n int64) (bool, Details[TInput, TKey], error) {
	return r.waitNWithDetails(ctx, input, ntime.Now(), n)
}

func (r *Limiters[TInput, TKey]) waitNWithDetails(
	ctx context.Context,
	input TInput,
	startTime ntime.Time,
	n int64,
) (bool, Details[TInput, TKey], error) {
	// currentTime is an approximation of the real clock moving forward.
	// It's imprecise because it depends on time.After below.
	currentTime := startTime

	for {
		allow, details := r.allowNWithDetails(input, currentTime, n)
		if allow {
			return allow, details, nil
		}

		retryAfter := details.RetryAfter()

		select {
		case <-ctx.Done():
			// Need to get updated details, since this cancellation
			// event might have been a while after the last call.
			allow, details := r.allowNWithDetails(input, currentTime, n)
			return allow, details, ctx.Err()
		case <-time.After(retryAfter):
			currentTime = currentTime.Add(retryAfter)
		}
	}
}

// WaitWithDebug will poll [Limiters.Allow] for a period of time,
// until it is cancelled by the passed context. It has the
// effect of adding latency to requests instead of refusing
// them immediately. Consider it graceful degradation.
//
// WaitWithDebug will return true if a token becomes available prior to
// the context cancellation, and will consume a token. It will
// return false if not, and therefore not consume a token. It will
// also return the debugs of the request.
//
// Take care to create an appropriate context. You almost certainly
// want [context.WithTimeout] or [context.WithDeadline].
//
// The returned error will be non-nil if the context is cancelled.
//
// WaitNWithDebug makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiters[TInput, TKey]) WaitWithDebug(
	ctx context.Context,
	input TInput,
) (bool, []Debug[TInput, TKey], error) {
	return r.WaitNWithDebug(ctx, input, 1)
}

// WaitNWithDebug will poll [Limiters.AllowN] for a period of time,
// until it is cancelled by the passed context. It has the
// effect of adding latency to requests instead of refusing
// them immediately. Consider it graceful degradation.
//
// WaitNWithDebug will return true if `n` tokens become available prior to
// the context cancellation, and will consume `n` tokens. If not,
// it will return false, and therefore consume no tokens. It will
// also return the debugs of the request.
//
// Take care to create an appropriate context. You almost certainly
// want [context.WithTimeout] or [context.WithDeadline].
//
// The returned error will be non-nil if the context is cancelled.
//
// WaitNWithDebug makes no ordering guarantees. Multiple concurrent calls may
// acquire tokens in any order.
func (r *Limiters[TInput, TKey]) WaitNWithDebug(
	ctx context.Context,
	input TInput,
	n int64,
) (bool, []Debug[TInput, TKey], error) {
	return r.waitNWithDebug(ctx, input, ntime.Now(), n)
}

func (r *Limiters[TInput, TKey]) waitNWithDebug(
	ctx context.Context,
	input TInput,
	startTime ntime.Time,
	n int64,
) (bool, []Debug[TInput, TKey], error) {
	// currentTime is an approximation of the real clock moving forward.
	// It's imprecise because it depends on time.After below.
	currentTime := startTime

	for {
		allow, debugs := r.allowNWithDebug(input, currentTime, n)
		if allow {
			return allow, debugs, nil
		}

		// len(debugs) will be greater than 0 here;
		// if there were zero, allow was true

		// Get the max retryAfter
		retryAfter := debugs[0].RetryAfter()
		for i := 1; i < len(debugs); i++ {
			debug := debugs[i]
			ra := debug.RetryAfter()
			if ra > retryAfter {
				retryAfter = ra
			}
		}

		select {
		case <-ctx.Done():
			// Need to get updated debugs, since this cancellation
			// event might have been a while after the last call.
			allow, debugs := r.allowNWithDebug(input, currentTime, n)
			return allow, debugs, ctx.Err()
		case <-time.After(retryAfter):
			currentTime = currentTime.Add(retryAfter)
		}
	}
}
