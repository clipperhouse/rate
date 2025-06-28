package ratelimiter_test

import (
	"testing"
	"time"

	"github.com/clipperhouse/ratelimiter"
	"github.com/stretchr/testify/require"
)

func TestLimit_DurationPerToken(t *testing.T) {
	limit := ratelimiter.NewLimit(10, time.Second)
	duration := limit.DurationPerToken

	require.Equal(t, time.Millisecond*100, duration, "duration per token should equal to period divided by count")
}
