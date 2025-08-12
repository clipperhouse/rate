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

	t.Run("SingleLimiter", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(5, time.Second)
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

		t.Run("Identical", func(t *testing.T) {
			t.Parallel()

			now := time.Now()

			limiter := NewLimiter(keyFunc, limit)
			limiters := NewLimiters(limiter)

			identical := NewLimiter(keyFunc, limit)

			// Test that Limiters with single Limiter behaves identically to direct Limiter usage

			// First request - both should allow
			allowed1, details1 := limiters.allowNWithDetails("test", now, 1)
			allowedDirect1, detailsDirect1 := identical.allowNWithDetails("test", now, 1)

			require.Equal(t, allowedDirect1, allowed1, "allow result should be identical")
			require.Equal(t, detailsDirect1.Allowed(), details1.Allowed(), "details.Allowed should be identical")
			require.Equal(t, detailsDirect1.TokensRequested(), details1.TokensRequested(), "tokensRequested should be identical")
			require.Equal(t, detailsDirect1.TokensConsumed(), details1.TokensConsumed(), "tokensConsumed should be identical")
			require.Equal(t, detailsDirect1.TokensRemaining(), details1.TokensRemaining(), "tokensRemaining should be identical")
			require.Equal(t, detailsDirect1.RetryAfter(), details1.RetryAfter(), "retryAfter should be identical")

			// Second request - both should allow
			allowed2, details2 := limiters.allowNWithDetails("test", now, 2)
			allowedDirect2, detailsDirect2 := identical.allowNWithDetails("test", now, 2)

			require.Equal(t, allowedDirect2, allowed2, "allow result should be identical for multi-token request")
			require.Equal(t, detailsDirect2.Allowed(), details2.Allowed(), "details.Allowed should be identical")
			require.Equal(t, detailsDirect2.TokensConsumed(), details2.TokensConsumed(), "tokensConsumed should be identical")
			require.Equal(t, detailsDirect2.TokensRemaining(), details2.TokensRemaining(), "tokensRemaining should be identical")
			require.Equal(t, detailsDirect2.RetryAfter(), details2.RetryAfter(), "retryAfter should be identical")

			// Third request - both should deny (bucket exhausted)
			allowed3, details3 := limiters.allowNWithDetails("test", now, 1)
			allowedDirect3, detailsDirect3 := identical.allowNWithDetails("test", now, 1)

			require.Equal(t, allowedDirect3, allowed3, "deny result should be identical")
			require.Equal(t, detailsDirect3.Allowed(), details3.Allowed(), "details.Allowed should be identical when denied")
			require.Equal(t, detailsDirect3.TokensConsumed(), details3.TokensConsumed(), "tokensConsumed should be identical when denied")
			require.Equal(t, detailsDirect3.TokensRemaining(), details3.TokensRemaining(), "tokensRemaining should be identical when denied")
			require.Equal(t, detailsDirect3.RetryAfter(), details3.RetryAfter(), "retryAfter should be identical when denied")
		})
	})

	t.Run("MultipleLimiters", func(t *testing.T) {
		t.Parallel()

		now := time.Now()

		// Limiter1: more restrictive per-second, generous per-minute
		perSecond1 := NewLimit(2, time.Second)  // 500ms/token
		perMinute1 := NewLimit(20, time.Minute) // 3s/token
		limiter1 := NewLimiter(keyFunc, perSecond1, perMinute1)

		// Limiter2: less restrictive per-second, more restrictive per-minute
		perSecond2 := NewLimit(3, time.Second)  // ~333ms/token
		perMinute2 := NewLimit(10, time.Minute) // 6s/token
		limiter2 := NewLimiter(keyFunc, perSecond2, perMinute2)

		limiters := NewLimiters(limiter1, limiter2)

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
		allowed3, d3 := limiters.allowNWithDetails("acct", now, 1)
		require.False(t, allowed3, "should be denied due to exhausted fastest bucket")
		require.False(t, d3.Allowed())
		require.Equal(t, int64(0), d3.TokensConsumed(), "no tokens consumed on denial")

		// RetryAfter should be max(next token wait across buckets). Per-second exhausted needs 500ms.
		// Other buckets still have tokens so their retryAfter <=0. Expect 500ms.
		perSecWait := perSecond1.DurationPerToken()
		require.Equal(t, perSecWait, d3.RetryAfter(), "retryAfter should be exactly one token duration for exhausted per-second bucket")
	})
}
