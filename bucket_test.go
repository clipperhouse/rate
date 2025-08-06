package rate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBucket_HasEnoughTokens(t *testing.T) {
	t.Parallel()

	now := time.Now()
	// One token at a time
	{
		limit := NewLimit(9, time.Second)
		bucket := newBucket(now, limit)

		for range limit.count {
			actual := bucket.hasTokens(now, limit, 1)
			require.True(t, actual, "expected to have enough tokens initially")
		}

		// Consume all tokens
		for range limit.count {
			bucket.consumeTokens(now, limit, 1)
		}

		actual := bucket.hasTokens(now, limit, 1)
		require.False(t, actual, "expected to have no tokens after consuming all")
	}

	// Multiple tokens
	{
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

func TestBucket_ConsumeToken(t *testing.T) {
	t.Parallel()
	now := time.Now()
	limit := NewLimit(11, time.Second)
	bucket := newBucket(now, limit)

	for i := range limit.count {
		{
			actual := bucket.remainingTokens(now, limit)
			expected := limit.count - i
			require.Equal(t, expected, actual, "remaining tokens expected consumption")
		}
		bucket.consumeTokens(now, limit, 1)
		{
			actual := bucket.remainingTokens(now, limit)
			expected := limit.count - i - 1
			require.Equal(t, expected, actual, "remaining tokens should be one less after consumption")
		}
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
}

func TestBucket_ConsumeToken_OldBucket(t *testing.T) {
	// ensure that an old bucket does proper accounting of consumed tokens
	// if the arithmetic is too naive, and old bucket would be mistakenly
	// interpreted as having more tokens that the limit
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

	// exhaust it quickly again
	for range limit.count {
		bucket.consumeTokens(executionTime, limit, 1)
	}
	require.False(t, bucket.hasTokens(executionTime, limit, 1), "expected to have no tokens after exhausting the old bucket")
}

func TestBucket_CheckCutoff(t *testing.T) {
	t.Parallel()
	now := time.Now()
	limit := NewLimit(13, time.Second)
	bucket := newBucket(now, limit)

	require.False(t, bucket.checkCutoff(now, limit), "expected no cutoff update on fresh bucket")

	bucket.consumeTokens(now, limit, 1)
	require.False(t, bucket.checkCutoff(now, limit), "expected no cutoff update bucket after consumption")

	now = now.Add(time.Hour)
	require.True(t, bucket.checkCutoff(now, limit), "expected cutoff update on old bucket")
}

func TestBucket_HasToken(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Tokens refill at ~111ms intervals
	limit := NewLimit(9, time.Second)
	bucket := newBucket(now, limit)

	for range limit.count * 2 {
		// any number of hasToken should return true, not mutate the bucket
		actual := bucket.hasTokens(now, limit, 1)
		require.True(t, actual, "expected to allow request with any number of hasToken calls")
	}

	// Consume all the tokens
	for range limit.count {
		bucket.consumeTokens(now, limit, 1)
	}

	require.False(t, bucket.hasTokens(now, limit, 1), "should have exhausted tokens")

	for range limit.count * 2 {
		// any number of hasToken should return false with no remaining tokens
		actual := bucket.hasTokens(now, limit, 1)
		require.False(t, actual, "expected all tokens to be gone")
	}

	// Refill one token
	now = now.Add(limit.durationPerToken)
	require.True(t, bucket.hasTokens(now, limit, 1), "hasToken should return true after token refill")
}
