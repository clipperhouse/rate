package rate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBucket_HasTokens(t *testing.T) {
	t.Parallel()

	// One token at a time
	{
		now := time.Now()
		const n = 1
		limit := NewLimit(9, time.Second)
		bucket := newBucket(now, limit)
		remaining := bucket.remainingTokens(now, limit)

		for range limit.count {
			actual := bucket.hasTokens(now, limit, n)
			require.True(t, actual, "expected to have enough tokens initially")
		}

		// any number of hasTokens should not mutate the bucket
		require.Equal(t, remaining, bucket.remainingTokens(now, limit), "remaining tokens should not change")

		// Consume all tokens
		for range limit.count {
			bucket.consumeTokens(now, limit, n)
		}

		actual := bucket.hasTokens(now, limit, n)
		require.False(t, actual, "expected to have no tokens after consuming all")

		// refill one token
		now = now.Add(limit.durationPerToken)
		actual = bucket.hasTokens(now, limit, n)
		require.True(t, actual, "expected to have one token after refilling")
	}

	// Multiple tokens
	{
		now := time.Now()
		limit := NewLimit(9, time.Second)
		bucket := newBucket(now, limit)

		bucket.consumeTokens(now, limit, 3)
		require.True(t, bucket.hasTokens(now, limit, 1), "expected to have at least 1 token after consuming 3")
		require.True(t, bucket.hasTokens(now, limit, 6), "expected to have 6 tokens after consuming 3")
		require.False(t, bucket.hasTokens(now, limit, 7), "expected not to have 7 tokens after consuming 3")

		bucket.consumeTokens(now, limit, 5)
		require.True(t, bucket.hasTokens(now, limit, 1), "expected to have at least 1 token after consuming 5")
		require.False(t, bucket.hasTokens(now, limit, 2), "expected not to have 2 tokens after consuming 5")
		require.False(t, bucket.hasTokens(now, limit, 7), "expected not to have 7 tokens after consuming 5")

		bucket.consumeTokens(now, limit, 5)
		require.False(t, bucket.hasTokens(now, limit, 1), "expected to have no tokens after consuming 5")
		require.False(t, bucket.hasTokens(now, limit, 2), "expected not to have 2 tokens after consuming 5")

		// now we're in debt by 4 tokens. not something one should do, but
		// bucket.consumeTokens is a simple primitive, it's up to the caller.

		// refill 6 tokens, netting +2 tokens
		now = now.Add(6 * limit.durationPerToken)
		require.True(t, bucket.hasTokens(now, limit, 1), "expected to have one token after refilling")
		require.True(t, bucket.hasTokens(now, limit, 2), "expected to have two tokens after refilling")
		require.False(t, bucket.hasTokens(now, limit, 3), "expected not to have 3 tokens after refilling")

		// consume 2 tokens
		bucket.consumeTokens(now, limit, 2)
		require.False(t, bucket.hasTokens(now, limit, 1), "expected not to have one token after consuming 2")
		require.False(t, bucket.hasTokens(now, limit, 2), "expected not to have 2 tokens after consuming 2")
	}
}

func TestBucket_RemainingTokens(t *testing.T) {
	t.Parallel()
	now := time.Now()
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	{
		actual := bucket.remainingTokens(now, limit)
		expected := limit.count
		require.Equal(t, expected, actual, "remaining tokens should equal to limit count")
	}

	// Age the bucket. If the arithmetic is naive, an old bucket would be mistakenly
	// interpreted as having more tokens than the limit.
	now = now.Add(time.Hour)
	{
		actual := bucket.remainingTokens(now, limit)
		expected := limit.count
		require.Equal(t, expected, actual, "remaining tokens should equal to limit count after a long time")
	}
}

func TestBucket_ConsumeTokens(t *testing.T) {
	t.Parallel()
	now := time.Now()
	limit := NewLimit(11, time.Second)
	bucket := newBucket(now, limit)

	for n := range int64(3) {
		before := bucket.remainingTokens(now, limit)
		bucket.consumeTokens(now, limit, n)
		after := bucket.remainingTokens(now, limit)
		actual := before - after
		expected := n
		require.Equal(t, expected, actual, "remaining tokens should be decremented by the number of consumed tokens")
	}

	t.Run("old bucket", func(t *testing.T) {
		t.Parallel()
		executionTime := time.Now()
		limit := NewLimit(11, time.Second)
		bucket := newBucket(executionTime, limit)

		// exhaust the bucket
		for range limit.count {
			bucket.consumeTokens(executionTime, limit, 1)
		}
		require.False(t, bucket.hasTokens(executionTime, limit, 1), "expected to have no tokens after exhausting the bucket")

		// age the bucket
		executionTime = executionTime.Add(time.Hour)

		// almost exhaust it
		for range limit.count - 1 {
			bucket.consumeTokens(executionTime, limit, 1)
		}
		require.True(t, bucket.hasTokens(executionTime, limit, 1), "expected to have one token after exhausting the old bucket")

		// consume the last token
		bucket.consumeTokens(executionTime, limit, 1)
		require.False(t, bucket.hasTokens(executionTime, limit, 1), "expected to have no tokens after consuming the last token")
	})
}

func TestBucket_NextTokensTime(t *testing.T) {
	t.Parallel()

	t.Run("SingleToken", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Initially should have all tokens available
		// The bucket starts at (now - period), so 1 token is available immediately
		nextTime := bucket.nextTokensTime(now, limit, 1)
		expected := now.Add(-limit.period).Add(limit.durationPerToken)
		require.Equal(t, expected, nextTime, "next tokens time should be calculated from bucket's full state")

		// Consume all tokens
		for range limit.count {
			bucket.consumeTokens(now, limit, 1)
		}

		// After consuming all 10 tokens, bucket.time = now - period + 10*durationPerToken
		// Next token should be available at bucket.time + 1*durationPerToken
		nextTime = bucket.nextTokensTime(now, limit, 1)
		expected = now.Add(-limit.period).Add(11 * limit.durationPerToken) // 10 consumed + 1 requested
		require.Equal(t, expected, nextTime, "next token should be available after one duration per token from bucket time")

		// Next 5 tokens should be available at bucket.time + 5*durationPerToken
		nextTime = bucket.nextTokensTime(now, limit, 5)
		expected = now.Add(-limit.period).Add(15 * limit.durationPerToken) // 10 consumed + 5 requested
		require.Equal(t, expected, nextTime, "next 5 tokens should be available after 5 * duration per token from bucket time")
	})

	t.Run("MultipleTokens", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Consume 3 tokens
		bucket.consumeTokens(now, limit, 3)

		// After consuming 3 tokens, the bucket time is now at (now - period + 3*durationPerToken)
		// When nextTokensTime is called, cutoff() returns bucket.time (since it's not before cutoff)
		// So nextTokensTime returns bucket.time + n*durationPerToken
		nextTime := bucket.nextTokensTime(now, limit, 1)
		expected := now.Add(-limit.period).Add(4 * limit.durationPerToken) // 3 consumed + 1 requested
		require.Equal(t, expected, nextTime, "next 1 token should be calculated from bucket time after consuming 3")

		// Next 7 tokens should be available at bucket.time + 7*durationPerToken
		nextTime = bucket.nextTokensTime(now, limit, 7)
		expected = now.Add(-limit.period).Add(10 * limit.durationPerToken) // 3 consumed + 7 requested
		require.Equal(t, expected, nextTime, "next 7 tokens should be calculated from bucket time after consuming 3")

		// Next 8 tokens should be available at bucket.time + 8*durationPerToken
		nextTime = bucket.nextTokensTime(now, limit, 8)
		expected = now.Add(-limit.period).Add(11 * limit.durationPerToken) // 3 consumed + 8 requested
		require.Equal(t, expected, nextTime, "next 8 tokens should be calculated from bucket time after consuming 3")

		// Next 10 tokens should be available at bucket.time + 10*durationPerToken
		nextTime = bucket.nextTokensTime(now, limit, 10)
		expected = now.Add(-limit.period).Add(13 * limit.durationPerToken) // 3 consumed + 10 requested
		require.Equal(t, expected, nextTime, "next 10 tokens should be calculated from bucket time after consuming 3")
	})

	t.Run("AgingBucket", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Consume all tokens
		for range limit.count {
			bucket.consumeTokens(now, limit, 1)
		}

		// Age the bucket significantly (more than the period)
		agedNow := now.Add(2 * limit.period)

		// Even though the bucket is old, nextTokensTime should respect the cutoff
		// and calculate from the cutoff time, not return spurious values.
		// The bucket should be considered "full" at the cutoff time.
		// For aged buckets, cutoff() returns (agedNow - period) since bucket.time is before cutoff
		nextTime := bucket.nextTokensTime(agedNow, limit, 1)
		expected := agedNow.Add(-limit.period).Add(limit.durationPerToken)
		require.Equal(t, expected, nextTime, "next token should be calculated from cutoff for aged bucket")

		nextTime = bucket.nextTokensTime(agedNow, limit, 5)
		expected = agedNow.Add(-limit.period).Add(5 * limit.durationPerToken)
		require.Equal(t, expected, nextTime, "next 5 tokens should be calculated from cutoff for aged bucket")

		nextTime = bucket.nextTokensTime(agedNow, limit, 10)
		expected = agedNow.Add(-limit.period).Add(10 * limit.durationPerToken)
		require.Equal(t, expected, nextTime, "next 10 tokens should be calculated from cutoff for aged bucket")

		// Even requesting more tokens than the limit should be calculated from cutoff
		nextTime = bucket.nextTokensTime(agedNow, limit, 15)
		expected = agedNow.Add(-limit.period).Add(15 * limit.durationPerToken)
		require.Equal(t, expected, nextTime, "next 15 tokens should be calculated from cutoff for aged bucket")
	})

	t.Run("PartiallyAgingBucket", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Consume all tokens
		for range limit.count {
			bucket.consumeTokens(now, limit, 1)
		}

		// Age the bucket by half the period
		halfAgedNow := now.Add(limit.period / 2)

		// Should still calculate from the bucket time, which is (now - period + 10*durationPerToken)
		// since the bucket is not old enough to trigger cutoff
		nextTime := bucket.nextTokensTime(halfAgedNow, limit, 1)
		expected := now.Add(-limit.period).Add(11 * limit.durationPerToken) // 10 consumed + 1 requested
		require.Equal(t, expected, nextTime, "next token should be calculated from bucket time for partially aged bucket")

		// Age the bucket by exactly the period
		exactlyAgedNow := now.Add(limit.period)

		// Now the bucket should be considered "full" at the cutoff
		// cutoff() returns (exactlyAgedNow - period) since bucket.time is before cutoff
		nextTime = bucket.nextTokensTime(exactlyAgedNow, limit, 1)
		expected = exactlyAgedNow.Add(-limit.period).Add(limit.durationPerToken)
		require.Equal(t, expected, nextTime, "next token should be calculated from cutoff for exactly aged bucket")
	})

	t.Run("NegativeTokens", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Consume some tokens first
		bucket.consumeTokens(now, limit, 5)

		// Then "add" tokens back with negative consumption
		bucket.consumeTokens(now, limit, -2)

		// After consuming 5 tokens and then adding 2 back, bucket.time = now - period + 3*durationPerToken
		// Should now have 7 tokens available at bucket.time + 7*durationPerToken
		nextTime := bucket.nextTokensTime(now, limit, 7)
		expected := now.Add(-limit.period).Add(10 * limit.durationPerToken) // 3 consumed + 7 requested
		require.Equal(t, expected, nextTime, "next 7 tokens should be calculated from bucket time after adding tokens back")

		// But 8 tokens should be available at bucket.time + 8*durationPerToken
		nextTime = bucket.nextTokensTime(now, limit, 8)
		expected = now.Add(-limit.period).Add(11 * limit.durationPerToken) // 3 consumed + 8 requested
		require.Equal(t, expected, nextTime, "next 8 tokens should be calculated from bucket time after adding tokens back")
	})
}

func TestBucket_RetryAfter(t *testing.T) {
	t.Parallel()

	t.Run("SingleToken", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Initially should have all tokens available, so retryAfter should be 0
		retryAfter := bucket.retryAfter(now, limit, 1)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when tokens are immediately available")

		// Consume all tokens
		for range limit.count {
			bucket.consumeTokens(now, limit, 1)
		}

		// After consuming all 10 tokens, next token should be available after 1*durationPerToken
		retryAfter = bucket.retryAfter(now, limit, 1)
		expected := limit.durationPerToken
		require.Equal(t, expected, retryAfter, "retryAfter should be durationPerToken for next token after consuming all")

		// Next 5 tokens should be available after 5*durationPerToken
		retryAfter = bucket.retryAfter(now, limit, 5)
		expected = 5 * limit.durationPerToken
		require.Equal(t, expected, retryAfter, "retryAfter should be 5*durationPerToken for next 5 tokens")
	})

	t.Run("MultipleTokens", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Consume 3 tokens
		bucket.consumeTokens(now, limit, 3)

		// Becuase there are tokens still available, retryAfter should be 0
		retryAfter := bucket.retryAfter(now, limit, 1)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 for next token after consuming 3")

		// Because there are tokens still available, retryAfter should be 0
		retryAfter = bucket.retryAfter(now, limit, 7)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 for next 7 tokens after consuming 3")

		// Now we're asking for too many tokens
		// one too many
		retryAfter = bucket.retryAfter(now, limit, 8)
		require.Equal(t, limit.durationPerToken, retryAfter, "retryAfter should be durationPerToken for next 8 tokens after consuming 3")

		// two too many
		retryAfter = bucket.retryAfter(now, limit, 9)
		require.Equal(t, 2*limit.durationPerToken, retryAfter, "retryAfter should be durationPerToken for next 8 tokens after consuming 3")
	})

	t.Run("AgingBucket", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Consume all tokens
		for range limit.count {
			bucket.consumeTokens(now, limit, 1)
		}

		// Age the bucket significantly (more than the period)
		agedNow := now.Add(2 * limit.period)

		// Even though the bucket is old, retryAfter should respect the cutoff
		// and calculate from the cutoff time. The bucket should be considered "full" at the cutoff.
		// For aged buckets, cutoff() returns (agedNow - period) since bucket.time is before cutoff
		// For 1 token: nextTokensTime = (agedNow - period) + 1*durationPerToken = agedNow - period + durationPerToken
		// retryAfter = max(0, (agedNow - period + durationPerToken) - agedNow) = max(0, -period + durationPerToken) = 0
		retryAfter := bucket.retryAfter(agedNow, limit, 1)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 for aged bucket requesting 1 token")

		// For 5 tokens: nextTokensTime = (agedNow - period) + 5*durationPerToken = agedNow - period + 5*durationPerToken
		// retryAfter = max(0, (agedNow - period + 5*durationPerToken) - agedNow) = max(0, -period + 5*durationPerToken) = 0
		retryAfter = bucket.retryAfter(agedNow, limit, 5)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 for aged bucket requesting 5 tokens")

		// For 10 tokens: nextTokensTime = (agedNow - period) + 10*durationPerToken = agedNow - period + 10*durationPerToken = agedNow
		// retryAfter = max(0, agedNow - agedNow) = 0
		retryAfter = bucket.retryAfter(agedNow, limit, 10)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 for aged bucket requesting 10 tokens")

		// Even requesting more tokens than the limit should be calculated from cutoff
		// For 15 tokens: nextTokensTime = (agedNow - period) + 15*durationPerToken = agedNow - period + 15*durationPerToken = agedNow + 5*durationPerToken
		// retryAfter = max(0, (agedNow + 5*durationPerToken) - agedNow) = 5*durationPerToken
		retryAfter = bucket.retryAfter(agedNow, limit, 15)
		expected := 5 * limit.durationPerToken
		require.Equal(t, expected, retryAfter, "retryAfter should be 5*durationPerToken for aged bucket requesting 15 tokens")
	})

	t.Run("PartiallyAgingBucket", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Consume all tokens
		for range limit.count {
			bucket.consumeTokens(now, limit, 1)
		}

		// Age the bucket by half the period
		halfAgedNow := now.Add(limit.period / 2)

		// Should still calculate from the bucket time, which is (now - period + 10*durationPerToken)
		// since the bucket is not old enough to trigger cutoff
		// For 1 token: nextTokensTime = bucket.time + 1*durationPerToken = (now - period + 10*durationPerToken) + 1*durationPerToken
		// = now - period + 11*durationPerToken
		// retryAfter = max(0, (now - period + 11*durationPerToken) - halfAgedNow) = max(0, (now - period + 11*durationPerToken) - (now + period/2))
		// = max(0, -period + 11*durationPerToken - period/2) = max(0, -1s + 1.1s - 0.5s) = max(0, -0.4s) = 0
		retryAfter := bucket.retryAfter(halfAgedNow, limit, 1)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 for partially aged bucket requesting 1 token")

		// Age the bucket by exactly the period
		exactlyAgedNow := now.Add(limit.period)

		// Now the bucket should be considered "full" at the cutoff
		// cutoff() returns (exactlyAgedNow - period) since bucket.time is before cutoff
		// For 1 token: nextTokensTime = (exactlyAgedNow - period) + 1*durationPerToken = exactlyAgedNow - period + durationPerToken
		// retryAfter = max(0, (exactlyAgedNow - period + durationPerToken) - exactlyAgedNow) = max(0, -period + durationPerToken) = 0
		retryAfter = bucket.retryAfter(exactlyAgedNow, limit, 1)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 for exactly aged bucket requesting 1 token")
	})

	t.Run("NegativeTokens", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Consume some tokens first
		bucket.consumeTokens(now, limit, 5)

		// Then "add" tokens back with negative consumption
		bucket.consumeTokens(now, limit, -2)

		// After consuming 5 tokens and then adding 2 back, bucket.time = now - period + 3*durationPerToken
		// For 7 tokens: nextTokensTime = bucket.time + 7*durationPerToken = now - period + 10*durationPerToken = now
		// retryAfter = max(0, now - now) = 0
		retryAfter := bucket.retryAfter(now, limit, 7)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 for next 7 tokens after adding tokens back")

		// For 8 tokens: nextTokensTime = bucket.time + 8*durationPerToken = now - period + 11*durationPerToken = now + durationPerToken
		// retryAfter = max(0, (now + durationPerToken) - now) = durationPerToken
		retryAfter = bucket.retryAfter(now, limit, 8)
		expected := limit.durationPerToken
		require.Equal(t, expected, retryAfter, "retryAfter should be durationPerToken for next 8 tokens after adding tokens back")
	})

	t.Run("ZeroTokens", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Requesting 0 tokens should return 0 duration
		retryAfter := bucket.retryAfter(now, limit, 0)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when requesting 0 tokens")

		// Even after consuming tokens, requesting 0 should still return 0
		bucket.consumeTokens(now, limit, 5)
		retryAfter = bucket.retryAfter(now, limit, 0)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when requesting 0 tokens even after consuming tokens")
	})

	t.Run("NegativeTokens", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Requesting negative tokens should return 0 duration (due to max(0, ...))
		retryAfter := bucket.retryAfter(now, limit, -1)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when requesting negative tokens")

		// Even after consuming tokens, requesting negative should still return 0
		bucket.consumeTokens(now, limit, 5)
		retryAfter = bucket.retryAfter(now, limit, -3)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when requesting negative tokens even after consuming tokens")
	})

	t.Run("TokensAvailable", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Initially all tokens are available, so any request should return 0
		retryAfter := bucket.retryAfter(now, limit, 1)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when 1 token is immediately available")

		retryAfter = bucket.retryAfter(now, limit, 5)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when 5 tokens are immediately available")

		retryAfter = bucket.retryAfter(now, limit, 10)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when 10 tokens are immediately available")

		// Consume some tokens but not all
		bucket.consumeTokens(now, limit, 3)

		// Should still have tokens available
		retryAfter = bucket.retryAfter(now, limit, 1)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when 1 token is still available after consuming 3")

		retryAfter = bucket.retryAfter(now, limit, 7)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when 7 tokens are still available after consuming 3")

		// But requesting more than available should return positive duration
		retryAfter = bucket.retryAfter(now, limit, 8)
		expected := limit.durationPerToken
		require.Equal(t, expected, retryAfter, "retryAfter should be durationPerToken when requesting 8 tokens after consuming 3")
	})

	t.Run("ConcurrentTime", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		limit := NewLimit(10, time.Second)
		bucket := newBucket(now, limit)

		// Consume all tokens
		for range limit.count {
			bucket.consumeTokens(now, limit, 1)
		}

		// Advance time by half a durationPerToken
		advancedNow := now.Add(limit.durationPerToken / 2)

		// Should still need half a durationPerToken more
		retryAfter := bucket.retryAfter(advancedNow, limit, 1)
		expected := limit.durationPerToken / 2
		require.Equal(t, expected, retryAfter, "retryAfter should be half durationPerToken when time has advanced by half")

		// Advance time by exactly durationPerToken
		exactlyAdvancedNow := now.Add(limit.durationPerToken)

		// Should now have 1 token available
		retryAfter = bucket.retryAfter(exactlyAdvancedNow, limit, 1)
		require.Equal(t, time.Duration(0), retryAfter, "retryAfter should be 0 when exactly durationPerToken has passed")

		// But should still need time for 2 tokens
		retryAfter = bucket.retryAfter(exactlyAdvancedNow, limit, 2)
		expected = limit.durationPerToken
		require.Equal(t, expected, retryAfter, "retryAfter should be durationPerToken when requesting 2 tokens after exactly one durationPerToken has passed")
	})
}
