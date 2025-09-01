package benchmarks

import (
	"math"
	"testing"

	"go.uber.org/ratelimit"
)

func BenchmarkUber(b *testing.B) {
	// Note that this benchmarks just one "bucket",
	// it's a smaller primitive than other implementations,
	// which manage multiple buckets.

	const tokens = math.MaxInt
	b.Run("serial", func(b *testing.B) {
		instance := ratelimit.New(tokens)
		b.ReportAllocs()
		b.ResetTimer()

		var x string
		for i := 0; i < b.N; i++ {
			x = testSessionID(b, i)
			instance.Take()
		}
		_ = x
	})

	b.Run("parallel", func(b *testing.B) {
		instance := ratelimit.New(tokens)
		b.ReportAllocs()
		b.ResetTimer()

		var x string
		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				x = testSessionID(b, i)
				instance.Take()
			}
		})
		_ = x
	})
}
