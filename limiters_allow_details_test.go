package rate

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiters_AllowNWithDetails(t *testing.T) {
	t.Parallel()

	keyFunc := func(input string) string {
		return fmt.Sprintf("bucket-%s", input)
	}

	t.Run("NoLimiters", func(t *testing.T) {
		t.Parallel()

		limiters := NewLimiters[string, string]()
		now := time.Now()

		allowed, d := limiters.allowNWithDetails("test", now, 1)
		require.True(t, allowed, "should allow when no limiters present")
		require.True(t, d.Allowed(), "details.Allowed should be true when no limiters present")
		require.Equal(t, int64(1), d.TokensRequested(), "tokens requested should match input")
		require.Equal(t, int64(0), d.TokensConsumed(), "no tokens consumed when unconstrained")
		require.Equal(t, int64(0), d.TokensRemaining(), "remaining tokens reported as 0 when unconstrained")
		require.Equal(t, time.Duration(0), d.RetryAfter(), "retryAfter should be 0 when allowed and unconstrained")
	})

	t.Run("SingleLimiter_Allowed", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(5, time.Second) // 200ms per token
		limiter := NewLimiter(keyFunc, limit)
		limiters := NewLimiters(limiter)
		now := time.Now()

		allowed, d := limiters.allowNWithDetails("test", now, 1)
		require.True(t, allowed)
		require.True(t, d.Allowed())
		require.Equal(t, int64(1), d.TokensRequested())
		require.Equal(t, int64(1), d.TokensConsumed())
		require.Equal(t, limit.Count()-1, d.TokensRemaining(), "remaining should decrement by 1")
		require.Equal(t, time.Duration(0), d.RetryAfter())
	})

	t.Run("SingleLimiter_Denied_MultiTokenRequest", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(5, time.Second) // 200ms/token
		limiter := NewLimiter(keyFunc, limit)
		limiters := NewLimiters(limiter)
		now := time.Now()

		// Consume all 5 tokens in one go
		allowed, d := limiters.allowNWithDetails("test", now, 5)
		require.True(t, allowed)
		require.Equal(t, int64(5), d.TokensConsumed())
		require.Equal(t, int64(0), d.TokensRemaining())

		// Request 2 tokens immediately (should be denied)
		deniedAllowed, deniedDetails := limiters.allowNWithDetails("test", now, 2)
		require.False(t, deniedAllowed)
		require.Equal(t, int64(0), deniedDetails.TokensConsumed())

		// Need 2 tokens => expect exactly 400ms wait (2 * 200ms)
		expected := 2 * limit.DurationPerToken()
		require.Equal(t, expected, deniedDetails.RetryAfter(), "retryAfter should be exactly 2 * durationPerToken for 2 tokens")
	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		t.Parallel()

		// Limiter1: more restrictive per-second, generous per-minute
		perSecond1 := NewLimit(2, time.Second)  // 500ms/token
		perMinute1 := NewLimit(20, time.Minute) // 3s/token
		limiter1 := NewLimiter(keyFunc, perSecond1, perMinute1)

		// Limiter2: less restrictive per-second, more restrictive per-minute
		perSecond2 := NewLimit(3, time.Second)  // ~333ms/token
		perMinute2 := NewLimit(10, time.Minute) // 6s/token
		limiter2 := NewLimiter(keyFunc, perSecond2, perMinute2)

		limiters := NewLimiters(limiter1, limiter2)
		now := time.Now()

		// First allow
		allowed1, d1 := limiters.allowNWithDetails("acct", now, 1)
		require.True(t, allowed1)
		require.True(t, d1.Allowed())

		// Remaining should be minimum across all 4 buckets:
		// perSecond1: 1 left, perMinute1: 19, perSecond2: 2, perMinute2: 9 => min=1
		require.Equal(t, int64(1), d1.TokensRemaining(), "remaining should be min across buckets")
		require.Equal(t, int64(1), d1.TokensConsumed())
		require.Equal(t, time.Duration(0), d1.RetryAfter(), "retryAfter should be 0 when allowed")

		// Second allow (consumes perSecond1 second token)
		allowed2, d2 := limiters.allowNWithDetails("acct", now, 1)
		require.True(t, allowed2)

		// Remaining now min across: perSecond1:0, perMinute1:18, perSecond2:1, perMinute2:8 => 0
		require.Equal(t, int64(0), d2.TokensRemaining(), "remaining should drop to 0 (most restrictive exhausted)")
		require.Equal(t, int64(1), d2.TokensConsumed())
		require.Equal(t, time.Duration(0), d2.RetryAfter())

		// Third allow denied (per-second bucket in limiter1 exhausted)
		deniedTime := now // same time => no refill yet
		allowed3, d3 := limiters.allowNWithDetails("acct", deniedTime, 1)
		require.False(t, allowed3, "should be denied due to exhausted fastest bucket")
		require.False(t, d3.Allowed())
		require.Equal(t, int64(0), d3.TokensConsumed(), "no tokens consumed on denial")

		// RetryAfter should be max(next token wait across buckets). Per-second exhausted needs 500ms.
		// Other buckets still have tokens so their retryAfter <=0. Expect 500ms.
		// Since we're using deterministic time (no real clock), this should be exact.
		perSecWait := perSecond1.DurationPerToken()
		require.Equal(t, perSecWait, d3.RetryAfter(), "retryAfter should be exactly one token duration for exhausted per-second bucket")
	})
}
