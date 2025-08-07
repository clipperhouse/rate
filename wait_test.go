package rate

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiter_Wait_SingleBucket(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Consume all tokens
	for range limit.count {
		ok := limiter.allow("test", executionTime)
		require.True(t, ok, "should allow initial tokens")
	}

	// Should not allow immediately
	ok := limiter.allow("test", executionTime)
	require.False(t, ok, "should not allow when tokens exhausted")

	// Test 1: Wait with enough time to acquire a token
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken), true
		}

		// Done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitWithCancellation("test", executionTime, deadline, done)
		require.True(t, allow, "should acquire token after waiting")

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)
	}

	// Should not allow again immediately after wait
	ok = limiter.allow("test", executionTime)
	require.False(t, ok, "should not allow again immediately after wait")

	// Test 2: Wait with deadline that expires before token is available
	{
		// Deadline that expires too soon
		deadlineTime := executionTime.Add(limit.durationPerToken / 2)
		deadline := func() (time.Time, bool) {
			return deadlineTime, true
		}

		// Done channel that closes when deadline expires
		doneCh := make(chan struct{})
		done := func() <-chan struct{} {
			return doneCh
		}

		// Simulate deadline expiration by closing the done channel after the deadline
		go func(startTime time.Time) {
			time.Sleep(deadlineTime.Sub(startTime))
			close(doneCh)
		}(executionTime)

		allow := limiter.waitWithCancellation("test", executionTime, deadline, done)
		require.False(t, allow, "should not acquire token if deadline expires before token is available")
	}

	// Test 3: Wait with immediate cancellation
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
			return ch
		}

		allow := limiter.waitWithCancellation("test", executionTime, deadline, done)
		require.False(t, allow, "should not acquire token if context is cancelled immediately")
	}

	// Test 4: Wait with no deadline
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
			allow := limiter.waitWithCancellation("test", executionTime, deadline, done)
			require.True(t, allow, "should acquire token when no deadline is set")
		}

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)

		// Should not allow again immediately
		{
			allow := limiter.allow("test", executionTime)
			require.False(t, allow, "should not allow again immediately after wait")
		}
	}
}

func TestLimiter_WaitN_SingleBucket(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Consume all tokens
	{
		ok := limiter.allowN("test", executionTime, limit.count)
		require.True(t, ok, "should allow initial tokens")
	}
	{
		// Should not allow immediately
		ok := limiter.allowN("test", executionTime, 1)
		require.False(t, ok, "should not allow when tokens exhausted")
	}

	// Test 1: Wait with enough time to acquire tokens
	{
		wait := time.Duration(limit.count) * limit.durationPerToken
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(wait), true
		}

		// Done channel that never closes (no cancellation)
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
		require.True(t, allow, "should acquire tokens after waiting")

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)
	}

	{
		// Should not allow again immediately after wait
		ok := limiter.allow("test", executionTime)
		require.False(t, ok, "should not allow again immediately after wait")
	}

	// Test 2: Wait with deadline that expires before n tokens are available
	{
		// Deadline that expires too soon (not enough time for all tokens to be refilled)
		deadlineTime := executionTime.Add(limit.durationPerToken)
		deadline := func() (time.Time, bool) {
			return deadlineTime, true
		}

		// Done channel that closes when deadline expires
		doneCh := make(chan struct{})
		done := func() <-chan struct{} {
			return doneCh
		}

		// Simulate deadline expiration by closing the done channel after the deadline
		go func(startTime time.Time) {
			time.Sleep(deadlineTime.Sub(startTime))
			close(doneCh)
		}(executionTime)

		allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
		require.False(t, allow, "should not acquire tokens if deadline expires before tokens are available")
	}

	// Test 3: Wait with immediate cancellation
	{
		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken * time.Duration(limit.count)), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
			return ch
		}

		allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
		require.False(t, allow, "should not acquire tokens if context is cancelled immediately")
	}

	// Test 4: Wait with no deadline
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
			allow := limiter.waitNWithCancellation("test", executionTime, limit.count, deadline, done)
			require.True(t, allow, "should acquire tokens when no deadline is set")
		}

		// We waited
		executionTime = executionTime.Add(limit.durationPerToken)

		// Should not allow again immediately
		{
			allow := limiter.allowN("test", executionTime, limit.count)
			require.False(t, allow, "should not allow again immediately after wait")
		}
	}
}

func TestLimiter_Wait_MultipleBuckets(t *testing.T) {
	t.Parallel()
	keyer := func(input int) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	const buckets = 3
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)
	executionTime := time.Now()

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
			return executionTime.Add(limit.durationPerToken), true
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
	keyer := func(input int64) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	const buckets int64 = 3
	limit := NewLimit(2, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)
	executionTime := time.Now()

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

	// Test 1: Multiple goroutines competing for tokens with limited time
	{
		// More goroutines than available tokens to create competition
		tokens := buckets * limit.count
		concurrency := tokens * 3 // oversubscribe by 3x
		results := make([]bool, concurrency)
		var wg sync.WaitGroup

		// Deadline that gives limited time - only enough for a few token refills
		// This creates real competition rather than allowing unlimited waiting
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.durationPerToken * 3), true // time for ~3 token refills
		}

		// Done channel that closes when deadline expires to simulate real context behavior
		doneCh := make(chan struct{})
		done := func() <-chan struct{} {
			return doneCh
		}

		// Simulate deadline expiration by closing the done channel after the deadline
		deadlineTime := executionTime.Add(limit.durationPerToken * 3)
		go func(startTime time.Time) {
			time.Sleep(deadlineTime.Sub(startTime))
			close(doneCh)
		}(executionTime)

		// Start concurrent waits
		for i := range concurrency {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				bucketID := i % buckets
				results[i] = limiter.waitWithCancellation(bucketID, executionTime, deadline, done)
			}(i)
		}

		wg.Wait()

		// We waited for the deadline period
		executionTime = executionTime.Add(limit.durationPerToken * 3)

		// Count successes and failures
		var successes, failures int64
		for _, result := range results {
			if result {
				successes++
			} else {
				failures++
			}
		}

		// With limited time, some goroutines should succeed (from token refills) but not all
		// We expect fewer successes than total goroutines due to the deadline
		require.Greater(t, successes, int64(0), "some goroutines should succeed")
		require.Less(t, successes, concurrency, "not all goroutines should succeed due to deadline")
		require.Equal(t, concurrency, successes+failures, "all goroutines should complete")
	}

	// Test 2: Multiple goroutines with deadline that expires before tokens are available
	{
		// Use fresh execution time for this test
		currentTime := time.Now()

		// Ensure all buckets are exhausted for this test
		for bucketID := range buckets {
			for range limit.count {
				limiter.allow(bucketID, currentTime)
			}
		}

		concurrency := buckets
		results := make([]bool, concurrency)
		var wg sync.WaitGroup

		// Deadline that expires too soon
		deadlineTime := currentTime.Add(limit.durationPerToken / 2)
		deadline := func() (time.Time, bool) {
			return deadlineTime, true
		}

		// Done channel that closes when deadline expires
		doneCh := make(chan struct{})
		done := func() <-chan struct{} {
			return doneCh
		}

		// Simulate deadline expiration by closing the done channel after the deadline
		go func(startTime time.Time) {
			time.Sleep(deadlineTime.Sub(startTime))
			close(doneCh)
		}(currentTime)

		// Start concurrent waits
		for i := range concurrency {
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				results[i] = limiter.waitWithCancellation(i, currentTime, deadline, done)
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
		var wg sync.WaitGroup

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(limit.period), true
		}

		// Done channel that closes immediately
		done := func() <-chan struct{} {
			ch := make(chan struct{})
			close(ch) // immediately closed
			return ch
		}

		// Start concurrent waits
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
	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(10, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Test 1: Wait should consume exactly 1 token
	t.Run("Wait_Consumes_One_Token", func(t *testing.T) {
		// Verify initial state
		_, initialDetails := limiter.peekWithDebug("test-wait-1", executionTime)
		require.Equal(t, limit.count, initialDetails[0].TokensRemaining(), "should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second), true
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
			return executionTime.Add(time.Second), true
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
		limiter := NewLimiter(keyer, perSecond, perMinute)
		const tokensToWait = 2

		// Verify initial state
		_, initialDetails := limiter.peekWithDebug("test-multi-waitn", executionTime)
		require.Len(t, initialDetails, 2, "should have details for both limits")
		require.Equal(t, perSecond.count, initialDetails[0].TokensRemaining(), "per-second should start with all tokens")
		require.Equal(t, perMinute.count, initialDetails[1].TokensRemaining(), "per-minute should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second), true
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
			return executionTime, true // expires immediately
		}

		// Done channel that closes immediately to simulate immediate deadline expiration
		doneCh := make(chan struct{})
		close(doneCh) // close immediately
		done := func() <-chan struct{} {
			return doneCh
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
		bigLimiter := NewLimiter(keyer, bigLimit)
		const tokensToWait = 25

		// Verify initial state
		_, initialDetails := bigLimiter.peekWithDebug("test-big-waitn", executionTime)
		require.Equal(t, bigLimit.count, initialDetails[0].TokensRemaining(), "should start with all tokens")

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second), true
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
		concurrentLimiter := NewLimiter(keyer, concurrentLimit)
		const tokensPerWait = 2
		const numGoroutines = 5             // Will try to consume 10 total tokens
		const expectedSuccesses = int64(10) // 20 / 2 = 10 successful waits possible

		results := make([]bool, numGoroutines)
		var wg sync.WaitGroup

		// Deadline that gives enough time
		deadline := func() (time.Time, bool) {
			return executionTime.Add(time.Second), true
		}
		done := func() <-chan struct{} {
			return make(chan struct{}) // never closes
		}

		// Start concurrent waits
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

func TestLimiter_Wait_FIFO_Ordering_SingleBucket(t *testing.T) {
	t.Parallel()
	keyer := func(input string) string {
		return "test-bucket"
	}
	// 1 token per 50ms
	limit := NewLimit(1, 50*time.Millisecond)
	limiter := NewLimiter(keyer, limit)
	executionTime := time.Now()

	// Exhaust the single token
	require.True(t, limiter.allow("key", executionTime), "should allow initial token")
	require.False(t, limiter.allow("key", executionTime), "should not allow second token")

	const concurrency = 5
	var wg sync.WaitGroup
	wg.Add(concurrency)

	startOrder := make(chan int, concurrency)
	successOrder := make(chan int, concurrency)

	// Deadline that gives enough time for all tokens to be refilled
	deadline := func() (time.Time, bool) {
		return executionTime.Add(time.Duration(concurrency) * limit.durationPerToken), true
	}

	// Done channel that never closes
	done := func() <-chan struct{} {
		return make(chan struct{}) // never closes
	}

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()

			// Signal that this goroutine is starting its wait
			startOrder <- id

			// Wait for a token
			if limiter.waitWithCancellation("key", executionTime, deadline, done) {
				// Signal that this goroutine successfully acquired a token
				successOrder <- id
			}
		}(i)
		// A small, non-deterministic delay to encourage goroutines to queue up in order.
		// This helps simulate a real-world scenario where requests arrive sequentially.
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()
	close(startOrder)
	close(successOrder)

	var starts []int
	for id := range startOrder {
		starts = append(starts, id)
	}

	var successes []int
	for id := range successOrder {
		successes = append(successes, id)
	}

	require.Equal(t, concurrency, len(successes), "all goroutines should have acquired a token")
	require.Equal(t, starts, successes, "success order should match start order, proving FIFO")
}

func TestLimiter_Wait_FIFO_Ordering_MultipleBuckets_Flaky(t *testing.T) {
	t.Parallel()

	// The FIFO behavior is best-effort, this test is known-flaky
	// as a result

	const buckets = 3
	const concurrencyPerBucket = 5
	const concurrency = buckets * concurrencyPerBucket

	keyer := func(input int) string {
		return fmt.Sprintf("test-bucket-%d", input)
	}
	// 1 token per 50ms, to make the test run reasonably fast
	limit := NewLimit(1, 50*time.Millisecond)
	limiter := NewLimiter(keyer, limit)
	executionTime := time.Now()

	// Exhaust the single token for each bucket
	for i := range buckets {
		require.True(t, limiter.allow(i, executionTime), "should allow initial token for bucket %d", i)
		require.False(t, limiter.allow(i, executionTime), "should not allow second token for bucket %d", i)
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)

	// Create maps to hold order channels for each bucket
	startOrders := make(map[int]chan int, buckets)
	successOrders := make(map[int]chan int, buckets)
	for i := range buckets {
		startOrders[i] = make(chan int, concurrencyPerBucket)
		successOrders[i] = make(chan int, concurrencyPerBucket)
	}

	// Deadline that gives enough time for all tokens to be refilled for all goroutines
	deadline := func() (time.Time, bool) {
		return executionTime.Add(time.Duration(concurrencyPerBucket) * limit.durationPerToken), true
	}

	// Done channel that never closes
	done := func() <-chan struct{} {
		return make(chan struct{}) // never closes
	}

	for i := range concurrency {
		go func(id int) {
			defer wg.Done()
			bucketID := id % buckets

			// Announce that this goroutine is starting its wait for its bucket
			startOrders[bucketID] <- id

			// Wait for a token
			if limiter.waitWithCancellation(bucketID, executionTime, deadline, done) {
				// Announce that this goroutine successfully acquired a token
				successOrders[bucketID] <- id
			}
		}(i)
		// A small, non-deterministic delay to encourage goroutines to queue up in order.
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()

	// Close all channels
	for bucketID := range buckets {
		close(startOrders[bucketID])
		close(successOrders[bucketID])
	}

	// Verify FIFO order for each bucket
	for bucketID := range buckets {
		var starts []int
		for id := range startOrders[bucketID] {
			starts = append(starts, id)
		}

		var successes []int
		for id := range successOrders[bucketID] {
			successes = append(successes, id)
		}

		require.Equal(t, concurrencyPerBucket, len(successes), "all goroutines for bucket %d should have acquired a token", bucketID)
		require.Equal(t, starts, successes, "success order should match start order for bucket %d, proving FIFO", bucketID)
	}
}

func TestLimiter_WaitersCleanup_Basic(t *testing.T) {
	t.Parallel()

	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(1, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	// Initial waiters count should be 0
	require.Equal(t, 0, limiter.waiters.count(), "initial waiters count should be 0")

	executionTime := time.Now()

	// Exhaust tokens for multiple keys
	require.True(t, limiter.allow("key1", executionTime), "should allow initial token for key1")
	require.True(t, limiter.allow("key2", executionTime), "should allow initial token for key2")

	// Create deadline that will timeout immediately
	deadline := func() (time.Time, bool) {
		return executionTime, true // immediate timeout
	}
	done := func() <-chan struct{} {
		ch := make(chan struct{})
		close(ch) // immediately cancelled
		return ch
	}

	// Try to wait - this should create a waiter entry that gets cleaned up
	allow := limiter.waitWithCancellation("key1", executionTime, deadline, done)
	require.False(t, allow, "should timeout immediately")

	// Check if waiter was cleaned up
	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after timeout")

	// Try again with a different key
	allow = limiter.waitWithCancellation("key2", executionTime, deadline, done)
	require.False(t, allow, "should timeout immediately")

	// Check waiters count again
	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after timeout")

	// Try the same key again
	allow = limiter.waitWithCancellation("key1", executionTime, deadline, done)
	require.False(t, allow, "should timeout immediately")

	// Check waiters count
	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after timeout")
}

func TestLimiter_WaitersCleanup_MemoryLeak_Prevention(t *testing.T) {
	t.Parallel()

	keyer := func(input int) string {
		return fmt.Sprintf("key-%d", input)
	}
	limit := NewLimit(1, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Create deadline that will timeout immediately
	deadline := func() (time.Time, bool) {
		return executionTime, true // immediate timeout
	}
	done := func() <-chan struct{} {
		ch := make(chan struct{})
		close(ch) // immediately cancelled
		return ch
	}

	const numKeys = 1000

	// Simulate many different keys trying to wait
	for i := range numKeys {
		// First exhaust the token for this key
		limiter.allow(i, executionTime)

		// Then try to wait (which will timeout immediately)
		result := limiter.waitWithCancellation(i, executionTime, deadline, done)
		require.False(t, result, "should timeout immediately for key %d", i)
	}

	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after use")
}

func TestLimiter_WaitersCleanup_Concurrent(t *testing.T) {
	t.Parallel()

	keyer := func(input int) string {
		return fmt.Sprintf("key-%d", input)
	}
	limit := NewLimit(1, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Create deadline that will timeout immediately
	deadline := func() (time.Time, bool) {
		return executionTime, true // immediate timeout
	}
	done := func() <-chan struct{} {
		ch := make(chan struct{})
		close(ch) // immediately cancelled
		return ch
	}

	const concurrency = 100
	const keysPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := range concurrency {
		go func(i int) {
			defer wg.Done()

			for k := range keysPerGoroutine {
				keyID := i*keysPerGoroutine + k

				// First exhaust the token for this key
				limiter.allow(keyID, executionTime)

				// Then try to wait (which will timeout immediately)
				limiter.waitWithCancellation(keyID, executionTime, deadline, done)
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, 0, limiter.waiters.count(), "waiters should be cleaned up after use")
}

func TestLimiter_WaitersCleanup_WithSuccessfulWaits(t *testing.T) {
	t.Parallel()

	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(1, 100*time.Millisecond)
	limiter := NewLimiter(keyer, limit)

	executionTime := time.Now()

	// Exhaust the token
	require.True(t, limiter.allow("key", executionTime), "should allow initial token")

	const concurrency = 20
	results := make([]bool, concurrency)

	// Create deadline that gives enough time for tokens to be refilled
	deadline := func() (time.Time, bool) {
		return executionTime.Add(time.Duration(concurrency) * limit.durationPerToken), true
	}

	done := func() <-chan struct{} {
		return make(chan struct{}) // never closes
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := range concurrency {
		go func(i int) {
			defer wg.Done()
			// stagger the start times
			time.Sleep(time.Duration(i) * time.Millisecond)
			results[i] = limiter.waitWithCancellation("key", executionTime, deadline, done)
		}(i)
	}
	wg.Wait()

	// All waiters should have eventually succeeded
	successes := 0
	for _, result := range results {
		if result {
			successes++
		}
	}

	require.Equal(t, concurrency, successes, "all waiters should eventually succeed")
	require.Equal(t, 0, limiter.waiters.count(), "all waiters should be cleaned up after completion")
}

func TestLimiter_Wait_FIFOOrdering_HighContention(t *testing.T) {
	t.Parallel()

	keyer := func(input int) int {
		return input
	}

	limiter := NewLimiter(keyer, NewLimit(1, 50*time.Millisecond))

	// Exhaust the bucket
	require.True(t, limiter.Allow(1))

	// Test that multiple waiters can successfully acquire tokens
	const waiters = 5
	results := make(chan bool, waiters)

	for range waiters {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			results <- limiter.Wait(ctx, 1)
		}()
	}

	// Collect results
	successes := 0
	for range waiters {
		select {
		case success := <-results:
			if success {
				successes++
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Test timed out")
		}
	}

	// Should get at least some successes (the fix should prevent token starvation)
	require.Greater(t, successes, 0, "Should have some successful token acquisitions")
	require.Equal(t, 0, limiter.waiters.count(), "All waiters should be cleaned up")
}

func TestLimiter_Wait_RaceCondition_Prevention(t *testing.T) {
	t.Parallel()

	keyer := func(input int) int {
		return input
	}
	// Use a very restrictive limit to force contention
	limiter := NewLimiter(keyer, NewLimit(1, 100*time.Millisecond))

	// Exhaust the bucket
	require.True(t, limiter.Allow(1))

	const concurrency = 50
	successes := int64(0)

	var wg sync.WaitGroup
	wg.Add(concurrency)
	// Start many goroutines that will compete for tokens
	for range concurrency {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			if limiter.Wait(ctx, 1) {
				atomic.AddInt64(&successes, 1)
			}
		}()
	}
	wg.Wait()

	// Should have some successes but not more than the tokens available
	// in the timeout period (roughly 20 tokens in 2 seconds at 100ms per token)
	actual := atomic.LoadInt64(&successes)
	require.Greater(t, actual, int64(0), "should have some successful acquisitions")
	require.LessOrEqual(t, actual, int64(25), "should not exceed reasonable token availability")

	// Verify no memory leaks
	require.Equal(t, 0, limiter.waiters.count(), "all waiters should be cleaned up")
}
