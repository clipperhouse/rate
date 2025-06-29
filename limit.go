package ratelimiter

import (
	"fmt"
	"time"
)

type limit struct {
	Count            int64
	Period           time.Duration
	durationPerToken time.Duration
}

func (l limit) String() string {
	return fmt.Sprintf("%d requests per %s", l.Count, l.Period.String())
}

// NewLimit creates a new limit with the given count and period.
// For example, to create a limit of 10 requests per second, use:
//
//	limit := ratelimiter.NewLimit(10, time.Second)
func NewLimit(count int64, period time.Duration) limit {
	return limit{
		Count:            count,
		Period:           period,
		durationPerToken: period / time.Duration(count),
	}
}
