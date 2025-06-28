package ratelimiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimit_DurationPerToken(t *testing.T) {
	limit := NewLimit(10, time.Second)
	actual := limit.durationPerToken
	expected := time.Millisecond * 100

	require.Equal(t, expected, actual, "duration per token should equal to period divided by count")
}
