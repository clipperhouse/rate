package rate

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/clipperhouse/ntime"
	"github.com/stretchr/testify/require"
)

func TestLimiter_Wait_SingleBucket(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input
	}
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyFunc, limit)

	executionTime := ntime.Now()

	// Consume all tokens
	for range limit.count {
		ok := limiter.allow("test", executionTime)
		require.True(t, ok, "should allow initial tokens")
	}

	// Should not allow immediately
	ok := limiter.allow("test", executionTime)
	require.False(t, ok, "should not allow when tokens exhausted")

	// Test 1: Wait for 1 token with enough time to acquire it
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken).ToTime(), true
		}

		// Done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitNWithCancellation("test", executionTime, 1, deadline, done)
		require.True(t, allow, "should acquire 1 token after waiting")

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)
	}

	// Should not allow again immediately after wait
	ok = limiter.allow("test", executionTime)
	require.False(t, ok, "should not allow again immediately after wait")

	// Test 2: Wait for 1 token with deadline that expires before token is available
	{
		// Deadline that expires too soon
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken / 2).ToTime(), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitNWithCancellation("test", executionTime, 1, deadline, done)
		require.False(t, allow, "should not acquire 1 token if deadline expires before token is available")
	}

	// Test 3: Wait for 1 token with immediate cancellation
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken).ToTime(), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
			return ch
		}

		allow := limiter.waitNWithCancellation("test", executionTime, 1, deadline, done)
		require.False(t, allow, "should not acquire 1 token if context is cancelled immediately")
	}

	// Test 4: Wait for 1 token with no deadline
	{
		// Deadline that returns no deadline
		deadline := func() (time.Time, bool) {
			return time.Time{}, false
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		{
			allow := limiter.waitNWithCancellation("test", executionTime, 1, deadline, done)
			require.True(t, allow, "should acquire 1 token when no deadline is set")
		}

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)

		// Should not allow again immediately
		{
			allow := limiter.allow("test", executionTime)
			require.False(t, allow, "should not allow again immediately after wait")
		}
	}

	// Test 5: Wait for multiple tokens (limit.count) with enough time to acquire them
	{
		// Wait for bucket to refill enough tokens
		executionTime = executionTime.Add(limit.durationPerToken * time.Duration(limit.count))

		wait := time.Duration(limit.count) * limit.durationPerToken
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(wait).ToTime(), true
		}

		// Done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
		require.True(t, allow, "should acquire %d tokens after waiting", limit.count)

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)
	}

	// Test 6: Wait for multiple tokens with deadline that expires before all tokens are available
	{
		// Wait for bucket to refill enough tokens
		executionTime = executionTime.Add(limit.durationPerToken * time.Duration(limit.count))

		// Consume all tokens to exhaust the bucket again
		for range limit.count {
			ok := limiter.allow("test", executionTime)
			require.True(t, ok, "should allow tokens to exhaust bucket")
		}

		// Deadline that expires too soon - use a very short duration
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken / 4).ToTime(), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
		require.False(t, allow, "should not acquire %d tokens if deadline expires before tokens are available", limit.count)
	}

	// Test 7: Wait for multiple tokens with immediate cancellation
	{
		// Wait for bucket to refill enough tokens
		executionTime = executionTime.Add(limit.durationPerToken * time.Duration(limit.count))

		// Consume all tokens to exhaust the bucket again
		for range limit.count {
			ok := limiter.allow("test", executionTime)
			require.True(t, ok, "should allow tokens to exhaust bucket")
		}

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken * time.Duration(limit.count)).ToTime(), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
			return ch
		}

		allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
		require.False(t, allow, "should not acquire %d tokens if context is cancelled immediately", limit.count)
	}

	// Test 8: Wait for multiple tokens with no deadline
	{
		// Wait for bucket to refill enough tokens
		executionTime = executionTime.Add(limit.durationPerToken * time.Duration(limit.count))

		// Consume all tokens to exhaust the bucket again
		for range limit.count {
			ok := limiter.allow("test", executionTime)
			require.True(t, ok, "should allow tokens to exhaust bucket")
		}

		// Deadline that returns no deadline
		deadline := func() (time.Time, bool) {
			return time.Time{}, false
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		{
			allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
			require.True(t, allow, "should acquire %d tokens when no deadline is set", limit.count)
		}

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)

		// Should not allow again immediately
		{
			allow := limiter.allowN("test", executionTime, limit.count)
			require.False(t, allow, "should not allow %d tokens again immediately after wait", limit.count)
		}
	}
}

func TestLimiter_Wait_MultipleBuckets(t *testing.T) {
	t.Parallel()
	keyFunc := func(input int) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	const buckets = 3
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyFunc, limit)
	executionTime := ntime.Now()

	// Exhaust tokens for all buckets
	for bucketID := range buckets {
		for range limit.count {
			allow := limiter.allow(bucketID, executionTime)
			require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
		}
	}

	// Should not allow immediately for any bucket
	for bucketID := range buckets {
		allow := limiter.allow(bucketID, executionTime)
		require.False(t, allow, "should not allow when tokens exhausted for bucket %d", bucketID)
	}

	// Wait for a token for each bucket
	for bucketID := range buckets {
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken).ToTime(), true
		}

		// Done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitWithCancellation(bucketID, executionTime, deadline, done)
		require.True(t, allow, "should acquire token after waiting for bucket %d", bucketID)
	}

	// We waited
	executionTime = executionTime.Add(limit.durationPerToken)

	// Buckets should be empty again
	for bucketID := range buckets {
		ok := limiter.allow(bucketID, executionTime)
		require.False(t, ok, "should not allow again immediately after wait for bucket %d", bucketID)
	}
}

func TestLimiter_Wait_MultipleBuckets_Concurrent(t *testing.T) {
	t.Parallel()
	keyFunc := func(input int64) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	const buckets int64 = 3
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyFunc, limit)
	executionTime := ntime.Now()

	// Exhaust tokens for all buckets
	for bucketID := range buckets {
		for range limit.count {
			allow := limiter.allow(bucketID, executionTime)
			require.True(t, allow, "should allow initial tokens for bucket %d", bucketID)
		}
	}

	// Buckets should be empty
	for bucketID := range buckets {
		allow := limiter.allow(bucketID, executionTime)
		require.False(t, allow, "should not allow when tokens exhausted for bucket %d", bucketID)
	}

	// Test 1: Multiple goroutines competing for tokens with enough time
	{
		// More goroutines than available tokens to create competition
		tokens := buckets * limit.count
		concurrency := tokens * 3 // oversubscribe by 3x
		results := make([]bool, concurrency)

		// Deadline that gives enough time for all tokens to be refilled
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.period).ToTime(), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// Start concurrent waits
		var wg sync.WaitGroup
		for i := range concurrency {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				bucketID := i % buckets
				results[i] = limiter.waitWithCancellation(bucketID, executionTime, deadline, done)
			}(i)
		}
		wg.Wait()

		// We waited
		executionTime = executionTime.Add(limit.period)

		// Count successes and failures
		var successes, failures int64
		for _, result := range results {
			if result {
				successes++
			} else {
				failures++
			}
		}

		// Exactly limit.count tokens per bucket should succeed, 3 buckets * 2 tokens each
		expectedSuccesses := buckets * limit.count
		require.Equal(t, expectedSuccesses, successes, "expected exactly %d goroutines to acquire tokens", expectedSuccesses)

		// The rest should fail due to competition
		expectedFailures := concurrency - expectedSuccesses
		require.Equal(t, expectedFailures, failures, "expected %d goroutines to fail due to competition", expectedFailures)
	}

	// Test 2: Multiple goroutines with deadline that expires before tokens are available
	{
		concurrency := buckets
		results := make([]bool, concurrency)

		// Deadline that expires too soon
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken / 2).ToTime(), true
		}

		// Done channel that never closes
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// Start concurrent waits
		var wg sync.WaitGroup
		for i := range concurrency {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				results[i] = limiter.waitWithCancellation(i, executionTime, deadline, done)
			}(i)
		}

		wg.Wait()

		// All should fail because deadline expires before tokens are available
		for i, result := range results {
			require.False(t, result, "goroutine %d should not acquire token due to early deadline", i)
		}
	}

	// Test 3: Multiple goroutines with immediate cancellation
	{
		concurrency := buckets
		results := make([]bool, concurrency)

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.period).ToTime(), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
			return ch
		}

		// Start concurrent waits
		var wg sync.WaitGroup
		for i := range concurrency {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				results[i] = limiter.waitWithCancellation(i, executionTime, deadline, done)
			}(i)
		}

		wg.Wait()

		// All should fail because context is cancelled immediately
		for i, result := range results {
			require.False(t, result, "goroutine %d should not acquire token due to immediate cancellation", i)
		}
	}
}

func TestLimiter_WaitN_ConsumesCorrectTokens(t *testing.T) {
	t.Parallel()
	keyFunc := func(input string) string {
		return input
	}
	limit := NewLimit(10, 100*time.Millisecond)
	limiter := NewLimiter(keyFunc, limit)

	executionTime := ntime.Now()

	// Test 1: Wait should consume exactly 1 token
	t.Run("Wait_Consumes_One_Token", func(t *testing.T) {
		// Verify initial state
		_, initialDetails := limiter.peekWithDebug("test-wait-1", executionTime)
		require.Equal(t, limit.count, initialDetails[0].TokensRemaining(), "should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second).ToTime(), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// Wait should succeed and consume exactly 1 token
		allowed := limiter.waitWithCancellation("test-wait-1", executionTime, deadline, done)
		require.True(t, allowed, "wait should succeed")

		// Verify exactly 1 token was consumed
		_, finalDetails := limiter.peekWithDebug("test-wait-1", executionTime)
		require.Equal(t, limit.count-1, finalDetails[0].TokensRemaining(), "should have consumed exactly 1 token")
	})

	// Test 2: WaitN should consume exactly n tokens
	t.Run("WaitN_Consumes_N_Tokens", func(t *testing.T) {
		const tokensToWait = 3

		// Verify initial state
		_, initialDetails := limiter.peekWithDebug("test-waitn-3", executionTime)
		require.Equal(t, limit.count, initialDetails[0].TokensRemaining(), "should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second).ToTime(), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// WaitN should succeed and consume exactly tokensToWait tokens
		allowed := limiter.waitNWithCancellation("test-waitn-3", executionTime, tokensToWait, deadline, done)
		require.True(t, allowed, "waitN should succeed")

		// Verify exactly tokensToWait tokens were consumed
		_, details := limiter.peekWithDebug("test-waitn-3", executionTime)
		require.Equal(t, limit.count-tokensToWait, details[0].TokensRemaining(), "should have consumed exactly %d tokens", tokensToWait)
	})

	// Test 3: WaitN with multiple limits should consume n tokens from all buckets
	t.Run("WaitN_MultipleLimits_Consumes_N_From_All", func(t *testing.T) {
		perSecond := NewLimit(5, time.Second)
		perMinute := NewLimit(20, time.Minute)
		limiter := NewLimiter(keyFunc, perSecond, perMinute)
		const tokensToWait = 2

		// Verify initial state
		_, initialDetails := limiter.peekWithDebug("test-multi-waitn", executionTime)
		require.Len(t, initialDetails, 2, "should have details for both limits")
		require.Equal(t, perSecond.count, initialDetails[0].TokensRemaining(), "per-second should start with all tokens")
		require.Equal(t, perMinute.count, initialDetails[1].TokensRemaining(), "per-minute should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second).ToTime(), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// WaitN should succeed and consume exactly tokensToWait tokens from both limits
		allowed := limiter.waitNWithCancellation("test-multi-waitn", executionTime, tokensToWait, deadline, done)
		require.True(t, allowed, "waitN should succeed with multiple limits")

		// Verify exactly tokensToWait tokens were consumed from both buckets
		_, finalDetails := limiter.peekWithDebug("test-multi-waitn", executionTime)
		require.Len(t, finalDetails, 2, "should have details for both limits")
		require.Equal(t, perSecond.count-tokensToWait, finalDetails[0].TokensRemaining(), "per-second should have consumed exactly %d tokens", tokensToWait)
		require.Equal(t, perMinute.count-tokensToWait, finalDetails[1].TokensRemaining(), "per-minute should have consumed exactly %d tokens", tokensToWait)
	})

	// Test 4: WaitN that fails should consume zero tokens
	t.Run("WaitN_Fails_Consumes_Zero_Tokens", func(t *testing.T) {
		// First, exhaust the bucket
		for range limit.count {
			limiter.allow("test-fail-waitn", executionTime)
		}

		// Verify bucket is exhausted
		_, exhaustedDetails := limiter.peekWithDebug("test-fail-waitn", executionTime)
		require.Equal(t, int64(0), exhaustedDetails[0].TokensRemaining(), "bucket should be exhausted")

		// Deadline that expires immediately (no time to refill)
		deadline := func() (time.Time, bool) {
			return executionTime.ToTime(), true // expires immediately
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// WaitN should fail and consume zero tokens
		allowed := limiter.waitNWithCancellation("test-fail-waitn", executionTime, 1, deadline, done)
		require.False(t, allowed, "waitN should fail when deadline expires before tokens available")

		// Verify no tokens were consumed
		_, finalDetails := limiter.peekWithDebug("test-fail-waitn", executionTime)
		require.Equal(t, int64(0), finalDetails[0].TokensRemaining(), "should still have 0 tokens after failed wait")
	})

	// Test 5: Verify WaitN with high token count
	t.Run("WaitN_High_Token_Count", func(t *testing.T) {
		bigLimit := NewLimit(50, time.Second)
		bigLimiter := NewLimiter(keyFunc, bigLimit)
		const tokensToWait = 25

		// Verify initial state
		_, initialDetails := bigLimiter.peekWithDebug("test-big-waitn", executionTime)
		require.Equal(t, bigLimit.count, initialDetails[0].TokensRemaining(), "should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second).ToTime(), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// WaitN should succeed and consume exactly tokensToWait tokens
		allowed := bigLimiter.waitNWithCancellation("test-big-waitn", executionTime, tokensToWait, deadline, done)
		require.True(t, allowed, "waitN should succeed with high token count")

		// Verify exactly tokensToWait tokens were consumed
		_, finalDetails := bigLimiter.peekWithDebug("test-big-waitn", executionTime)
		require.Equal(t, bigLimit.count-tokensToWait, finalDetails[0].TokensRemaining(), "should have consumed exactly %d tokens", tokensToWait)
	})

	// Test 6: Concurrent WaitN should consume correct total tokens
	t.Run("WaitN_Concurrent_Token_Consumption", func(t *testing.T) {
		concurrentLimit := NewLimit(20, time.Second)
		concurrentLimiter := NewLimiter(keyFunc, concurrentLimit)
		const tokensPerWait = 2
		const numGoroutines = 5             // Will try to consume 10 total tokens
		const expectedSuccesses = int64(10) // 20 / 2 = 10 successful waits possible

		results := make([]bool, numGoroutines)

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second).ToTime(), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// Start concurrent waits
		var wg sync.WaitGroup
		for i := range numGoroutines {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i] = concurrentLimiter.waitNWithCancellation("test-concurrent-waitn", executionTime, tokensPerWait, deadline, done)
			}(i)
		}
		wg.Wait()

		// Count successes
		var successes int64
		for _, result := range results {
			if result {
				successes++
			}
		}

		require.Equal(t, expectedSuccesses/tokensPerWait, successes, "expected exactly %d successful waits", expectedSuccesses/tokensPerWait)

		// Verify total tokens consumed
		_, finalDetails := concurrentLimiter.peekWithDebug("test-concurrent-waitn", executionTime)
		expectedRemaining := concurrentLimit.count - (successes * tokensPerWait)
		require.Equal(t, expectedRemaining, finalDetails[0].TokensRemaining(), "should have consumed exactly %d tokens total", successes*tokensPerWait)
	})
}
