package rate

import (
	"sync"
	"testing"
	"time"
)

func TestLimiter_ConcurrentAccess_SyncMap(t *testing.T) {
	t.Parallel()

	keyer := func(input string) string {
		return input
	}
	limit := NewLimit(100, time.Second)
	limiter := NewLimiter(keyer, limit)

	const concurrency = 100
	const ops = 1000

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for range ops {
				// Test Allow method
				limiter.Allow("test-key")

				// Test Peek method
				limiter.Peek("test-key")

				// Test AllowWithDetails method
				limiter.AllowWithDetails("test-key")

				// Test PeekWithDetails method
				limiter.PeekWithDetails("test-key")
			}
		}()
	}
	wg.Wait()

	// If we get here without panic or race conditions, the test passes
	t.Log("Concurrent access test completed successfully")
}
