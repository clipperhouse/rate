package benchmarks

import (
	"math"
	"testing"
	"time"

	"github.com/didip/tollbooth/v6/limiter"
)

func BenchmarkTollbooth(b *testing.B) {
	const tokens = math.MaxFloat64
	const sweepMinTTL = time.Duration(math.MaxInt64)

	b.Run("serial", func(b *testing.B) {
		// Note: tollbooth doesn't support any granularity lower than 1 second
		instance := limiter.New(nil).
			SetMax(tokens).
			SetTokenBucketExpirationTTL(sweepMinTTL)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			instance.LimitReached(testSessionID(b, i))
		}
	})

	b.Run("parallel", func(b *testing.B) {
		// Note: tollbooth doesn't support any granularity lower than 1 second
		instance := limiter.New(nil).
			SetMax(tokens).
			SetTokenBucketExpirationTTL(sweepMinTTL)

		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				instance.LimitReached(testSessionID(b, i))
			}
		})
	})
}
