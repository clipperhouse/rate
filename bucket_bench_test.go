package rate

import (
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// Benchmark single allow calls for this package
func BenchmarkAllow_Single(b *testing.B) {
	lim := NewLimit(100, time.Second)
	bucket := newBucket(time.Now(), lim)
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		bucket.allow(now, lim)
	}
}

// Benchmark single allow calls for golang.org/x/time/rate
func BenchmarkRate_Allow_Single(b *testing.B) {
	limiter := rate.NewLimiter(rate.Limit(100), 0)
	now := time.Now()

	b.ReportAllocs()

	for b.Loop() {
		limiter.AllowN(now, 1)
	}
}
