package benchmarks

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/sethvargo/go-limiter/memorystore"
)

func BenchmarkSethVargo(b *testing.B) {
	ctx := context.Background()

	const tokens = math.MaxUint64
	const interval = time.Duration(math.MaxInt64)
	const sweepInterval = time.Duration(math.MaxInt64)
	const sweepMinTTL = time.Duration(math.MaxInt64)

	b.Run("serial", func(b *testing.B) {
		store, err := memorystore.New(&memorystore.Config{
			SweepInterval: sweepInterval,
			SweepMinTTL:   sweepMinTTL,
			Interval:      interval,
			Tokens:        tokens,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := store.Close(ctx); err != nil {
				b.Fatal(err)
			}
		})

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			store.Take(ctx, testSessionID(b, i))
		}
		b.StopTimer()
	})

	b.Run("parallel", func(b *testing.B) {
		store, err := memorystore.New(&memorystore.Config{
			SweepInterval: sweepInterval,
			SweepMinTTL:   sweepMinTTL,
			Interval:      interval,
			Tokens:        tokens,
		})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := store.Close(ctx); err != nil {
				b.Fatal(err)
			}
		})
		b.ReportAllocs()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				store.Take(ctx, testSessionID(b, i))
			}
		})
		b.StopTimer()
	})
}
