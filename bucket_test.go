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

	t.Run("basic single token", func(t *testing.T) {
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

	t.Run("multiple tokens", func(t *testing.T) {
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

	t.Run("aging bucket respects cutoff", func(t *testing.T) {
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

	t.Run("partially aged bucket", func(t *testing.T) {
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

	t.Run("negative token consumption", func(t *testing.T) {
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
