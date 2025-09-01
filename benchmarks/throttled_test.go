package benchmarks

import (
	"testing"

	"github.com/throttled/throttled"
	"github.com/throttled/throttled/store/memstore"
)

func BenchmarkThrottled(b *testing.B) {
	const tokens = 1000000000 // throttled overflows larger than this

	b.Run("serial", func(b *testing.B) {
		store, err := memstore.New(4096)
		if err != nil {
			b.Fatal(err)
		}

		quota := throttled.RateQuota{MaxRate: throttled.PerSec(tokens)}
		limiter, err := throttled.NewGCRARateLimiter(store, quota)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			limiter.RateLimit(testSessionID(b, i), 1)
		}
	})

	b.Run("parallel", func(b *testing.B) {
		store, err := memstore.New(4096)
		if err != nil {
			b.Fatal(err)
		}

		quota := throttled.RateQuota{MaxRate: throttled.PerSec(tokens)}
		limiter, err := throttled.NewGCRARateLimiter(store, quota)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				limiter.RateLimit(testSessionID(b, i), 1)
			}
		})
	})
}
