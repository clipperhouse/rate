package rate

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSyncMap_ConcurrentAccess(t *testing.T) {
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

				// Test AllowWithDebug method
				limiter.AllowWithDebug("test-key")

				// Test PeekWithDebug method
				limiter.PeekWithDebug("test-key")
			}
		}()
	}
	wg.Wait()

	// If we get here without panic or race conditions, the test passes
}

func TestSyncMap_LoadOrStoreFunc(t *testing.T) {
	t.Parallel()

	sm := &syncMap[string, int]{}

	factoryCalls := 0
	factory := func() int {
		factoryCalls++
		return 42
	}

	// First call should call factory and store the value
	result1 := sm.loadOrStore("key1", factory)
	require.Equal(t, 42, result1, "should return factory value")
	require.Equal(t, 1, factoryCalls, "factory should be called once")

	// Second call with same key should NOT call factory
	result2 := sm.loadOrStore("key1", factory)
	require.Equal(t, 42, result2, "should return existing value")
	require.Equal(t, 1, factoryCalls, "factory should not be called again for existing key")

	// Third call with different key should call factory again
	result3 := sm.loadOrStore("key2", factory)
	require.Equal(t, 42, result3, "should return factory value for new key")
	require.Equal(t, 2, factoryCalls, "factory should be called for new key")
}

func TestSyncMap_Count(t *testing.T) {
	t.Parallel()

	sm := &syncMap[string, int]{}

	for range 2 {
		// should only be stored once despite multiple calls
		for key := range 101 {
			sm.loadOrStore(fmt.Sprint(key), func() int {
				return key
			})
		}
	}

	expected := 101
	actual := sm.count()
	require.Equal(t, expected, actual, "expected Count() to be accurate")
}
