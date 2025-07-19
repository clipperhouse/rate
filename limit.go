package rate

import (
	"fmt"
	"time"
)

type Limit struct {
	count            int64
	period           time.Duration
	durationPerToken time.Duration
}

type LimitFunc[TInput any] func(input TInput) Limit

// NewLimit creates a new rate with the given count and period.
// For example, to create a rate of 10 requests per second, use:
//
//	limit := rate.NewLimit(10, time.Second)
func NewLimit(count int64, period time.Duration) Limit {
	return Limit{
		count:            count,
		period:           period,
		durationPerToken: period / time.Duration(count),
	}
}

func (l Limit) String() string {
	return "Limit{" +
		"count: " + fmt.Sprint(l.count) +
		", period: " + l.period.String() +
		", durationPerToken: " + l.durationPerToken.String() +
		"}"
}

func (l Limit) Count() int64 {
	return l.count
}

func (l Limit) Period() time.Duration {
	return l.period
}

func (l Limit) DurationPerToken() time.Duration {
	return l.durationPerToken
}
