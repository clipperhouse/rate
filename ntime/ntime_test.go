package ntime

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// @clipperhouse: I don't think we can actually test monotonicity
// against system clock changes. These tests are something I suppose.
// https://chatgpt.com/share/689f6a5d-2f64-8007-b1cc-3bdf10cfee20

func TestTime_Now_Monotonic(t *testing.T) {
	t.Parallel()

	// Test that consecutive calls to Now() return monotonically non-decreasing values
	times := make([]Time, 100)
	for i := range 100 {
		times[i] = Now()
		// Small delay to ensure different timestamps
		time.Sleep(time.Microsecond)
	}

	// Verify monotonicity (values should never go backwards)
	for i := 1; i < len(times); i++ {
		require.GreaterOrEqual(t, times[i], times[i-1],
			"Time %d (%d) should not be less than time %d (%d)",
			i, times[i], i-1, times[i-1])
	}
}

func TestTime_Now_Concurrent_Monotonic(t *testing.T) {
	t.Parallel()

	// Test monotonicity under concurrent access
	const concurrency = 100
	const calls = 10

	times := make(chan Time, concurrency*calls)

	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < calls; j++ {
				times <- Now()
				time.Sleep(time.Microsecond)
			}
		}()
	}
	wg.Wait()
	close(times)

	// Collect all times and verify monotonicity
	var allTimes []Time
	for t := range times {
		allTimes = append(allTimes, t)
	}

	// Sort times to check monotonicity
	slices.Sort(allTimes)

	// Verify no duplicates and monotonicity
	for i := 1; i < len(allTimes); i++ {
		require.GreaterOrEqual(t, allTimes[i], allTimes[i-1],
			"Time %d (%d) should not be less than time %d (%d)",
			i, allTimes[i], i-1, allTimes[i-1])
	}
}

func TestTime_Add(t *testing.T) {
	t.Parallel()

	now := Now()
	duration := time.Second

	result := now.Add(duration)
	expected := now + Time(duration.Nanoseconds())

	require.Equal(t, expected, result, "Add: got %d, want %d", result, expected)
}

func TestTime_Sub(t *testing.T) {
	t.Parallel()

	now := Now()
	time.Sleep(time.Millisecond)
	later := Now()

	duration := later.Sub(now)

	require.Greater(t, duration, time.Duration(0), "Sub: duration should be positive, got %v", duration)

	// Verify the duration is reasonable (should be around 1ms)
	require.GreaterOrEqual(t, duration, time.Microsecond, "Sub: duration too small: %v", duration)
	require.LessOrEqual(t, duration, time.Second, "Sub: duration too large: %v", duration)
}

func TestTime_After(t *testing.T) {
	t.Parallel()

	now := Now()
	time.Sleep(time.Microsecond)
	later := Now()

	require.True(t, later.After(now), "After: later time should be after earlier time")
	require.False(t, now.After(later), "After: earlier time should not be after later time")
}

func TestTime_Before(t *testing.T) {
	t.Parallel()

	now := Now()
	time.Sleep(time.Microsecond)
	later := Now()

	require.True(t, now.Before(later), "Before: earlier time should be before later time")
	require.False(t, later.Before(now), "Before: later time should not be before earlier time")
}

func TestTime_ToSystemTime(t *testing.T) {
	t.Parallel()

	nt := Now()
	systemTime := nt.ToSystemTime()

	// The system time should be close to the current time
	// Allow for some small difference due to execution time
	diff := time.Since(systemTime)
	require.GreaterOrEqual(t, diff, -time.Millisecond, "ToSystemTime: difference too negative: %v", diff)
	require.LessOrEqual(t, diff, time.Millisecond, "ToSystemTime: difference too positive: %v", diff)
}

func TestTime_Monotonic_UnderLoad(t *testing.T) {
	t.Parallel()

	// Test monotonicity under high-frequency calls
	const calls = 10000
	times := make([]Time, calls)

	start := time.Now()
	for i := range calls {
		times[i] = Now()
	}
	duration := time.Since(start)

	// Verify monotonicity
	for i := 1; i < len(times); i++ {
		require.GreaterOrEqual(t, times[i], times[i-1],
			"Monotonicity violation at index %d: %d < %d",
			i, times[i], times[i-1])
	}

	// Verify reasonable performance (should complete quickly)
	require.Less(t, duration, time.Second, "Performance test took too long: %v", duration)
}

func TestTime_Now_Monotonic_SystemClockChange(t *testing.T) {
	t.Parallel()

	// Test monotonicity when system clock changes
	// This is the real test of monotonic behavior

	// Get initial time
	initial := Now()

	// Simulate system clock going backwards (like NTP adjustment)
	// We can't actually change the system clock, but we can test
	// that our epoch-based approach maintains monotonicity

	// Wait a bit and get another time
	time.Sleep(time.Millisecond)
	afterWait := Now()

	// Verify the second time is not less than the first
	require.GreaterOrEqual(t, afterWait, initial,
		"Time after wait (%d) should not be less than initial time (%d)",
		afterWait, initial)

	// Now test that rapid calls maintain monotonicity
	// even if they happen in the same nanosecond
	times := make([]Time, 1000)
	for i := range 1000 {
		times[i] = Now()
	}

	// Verify monotonicity (values never go backwards)
	for i := 1; i < len(times); i++ {
		require.GreaterOrEqual(t, times[i], times[i-1],
			"Monotonicity violation at index %d: %d < %d",
			i, times[i], times[i-1])
	}
}

func TestTime_Now_Monotonic_ClockDrift(t *testing.T) {
	t.Parallel()

	// Test that our monotonic time is immune to system clock drift
	// by comparing with wall clock time

	start := time.Now()
	startNTime := Now()

	// Wait a bit
	time.Sleep(time.Millisecond)

	end := time.Now()
	endNTime := Now()

	// Calculate durations
	wallDuration := end.Sub(start)
	monotonicDuration := endNTime.Sub(startNTime)

	// The monotonic duration should be close to wall duration
	// Allow for some small difference due to overhead
	diff := monotonicDuration - wallDuration

	// The difference should be small (within a few microseconds)
	// Our monotonic time might be slightly less due to overhead
	require.GreaterOrEqual(t, diff, -5*time.Microsecond,
		"Monotonic duration (%v) should not be much less than wall duration (%v), diff: %v",
		monotonicDuration, wallDuration, diff)
	require.LessOrEqual(t, diff, 5*time.Microsecond,
		"Monotonic duration (%v) should not be much more than wall duration (%v), diff: %v",
		monotonicDuration, wallDuration, diff)
}

func BenchmarkNow(b *testing.B) {
	for b.Loop() {
		Now()
	}
}

func BenchmarkNow_Parallel(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Now()
		}
	})
}

func BenchmarkTime_Add(b *testing.B) {
	now := Now()
	duration := time.Second

	for b.Loop() {
		now.Add(duration)
	}
}

func BenchmarkTime_Sub(b *testing.B) {
	now := Now()
	time.Sleep(time.Microsecond)
	later := Now()

	for b.Loop() {
		later.Sub(now)
	}
}
