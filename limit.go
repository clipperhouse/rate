package rate

import (
	"time"
)

type Limit struct {
	period           time.Duration
	durationPerToken time.Duration
}

type LimitFunc[TInput any] func(input TInput) Limit

// NewLimit creates a new rate with the given count and period.
// For example, to create a rate of 10 requests per second, use:
//
//	limit := rate.NewLimit(10, time.Second)
func NewLimit(tokens int64, period time.Duration) Limit {
	return Limit{
		period:           period,
		durationPerToken: period / time.Duration(tokens),
	}
}

func (l Limit) Count() int64 {
	return int64(l.period / l.durationPerToken)
}

func (l Limit) Period() time.Duration {
	return l.period
}

func (l Limit) DurationPerToken() time.Duration {
	return l.durationPerToken
}
