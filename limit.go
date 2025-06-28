package ratelimiter

import "time"

type limit struct {
	Count            int64
	Period           time.Duration
	DurationPerToken time.Duration
}

func NewLimit(count int64, period time.Duration) limit {
	return limit{
		Count:            count,
		Period:           period,
		DurationPerToken: period / time.Duration(count),
	}
}
