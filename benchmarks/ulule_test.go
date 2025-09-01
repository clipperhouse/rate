package benchmarks

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/ulule/limiter/v3"
	"github.com/ulule/limiter/v3/drivers/store/memory"
)

func BenchmarkUlule(b *testing.B) {
	const tokens = math.MaxInt64
	const interval = time.Duration(math.MaxInt64)

	b.Run("serial", func(b *testing.B) {
		ctx := context.Background()
		store := memory.NewStore()
		instance := limiter.New(store, limiter.Rate{
			Limit:  tokens,
			Period: interval,
		})
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			instance.Get(ctx, testSessionID(b, i))
		}
	})

	b.Run("parallel", func(b *testing.B) {
		ctx := context.Background()
		store := memory.NewStore()
		instance := limiter.New(store, limiter.Rate{
			Limit:  tokens,
			Period: interval,
		})
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				instance.Get(ctx, testSessionID(b, i))
			}
		})
	})
}
