package benchmarks

import (
	"math"
	"testing"
	"time"

	"github.com/clipperhouse/rate"
)

func BenchmarkClipperhouse(b *testing.B) {
	const tokens = math.MaxInt64
	const interval = time.Duration(math.MaxInt64)

	keyFunc := func(i int) int {
		return i % NumSessions
	}
	limit := rate.NewLimit(tokens, interval)

	b.Run("serial", func(b *testing.B) {
		limiter := rate.NewLimiter(keyFunc, limit)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			limiter.Allow(i)
		}
		b.StopTimer()
	})

	b.Run("parallel", func(b *testing.B) {
		limiter := rate.NewLimiter(keyFunc, limit)
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				limiter.Allow(i)
			}
		})
		b.StopTimer()
	})
}
