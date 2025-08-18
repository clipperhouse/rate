package rate

import (
	"math"
	"testing"
	"time"

	"github.com/clipperhouse/ntime"
)

func BenchmarkLimiter_Serial_Parallel(b *testing.B) {
	keyer := func(i int) int {
		return i % 1000
	}
	limit := NewLimit(math.MaxInt64, time.Duration(math.MaxInt64))

	b.Run("Serial", func(b *testing.B) {
		limiter := NewLimiter(keyer, limit)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			limiter.Allow(i)
		}
		b.StopTimer()
	})

	b.Run("Parallel", func(b *testing.B) {
		limiter := NewLimiter(keyer, limit)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				limiter.Allow(i)
			}
		})
		b.StopTimer()
	})
}

func BenchmarkLimiter_Allow(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string { return "allow-single-bucket" }

		b.Run("SingleLimit", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				now := ntime.Now()

				b.ReportAllocs()

				for b.Loop() {
					limiter.allowN(0, now, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				now := ntime.Now()

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.allowN(0, now, 1)
					}
				})
			})
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				now := ntime.Now()

				b.ReportAllocs()

				for b.Loop() {
					limiter.allowN(0, now, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				now := ntime.Now()

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.allowN(0, now, 1)
					}
				})
			})
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyFunc := func(id int) int { return id }

		b.Run("SingleLimit", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				now := ntime.Now()

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; b.Loop(); i++ {
					limiter.allowN(i%buckets, now, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				now := ntime.Now()

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.allowN(i%buckets, now, 1)
						i++
					}
				})
			})
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				now := ntime.Now()

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; b.Loop(); i++ {
					limiter.allowN(i%buckets, now, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				now := ntime.Now()

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.allowN(i%buckets, now, 1)
						i++
					}
				})
			})
		})
	})
}

func BenchmarkLimiter_AllowWithDetails(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string { return "allow-details-single-bucket" }

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.allowNWithDetails(0, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyFunc, limit1, limit2)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.allowNWithDetails(0, now, 1)
			}
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyFunc := func(id int) int { return id }

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.allowNWithDetails(i%buckets, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyFunc, limit1, limit2)
			now := ntime.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.allowNWithDetails(i%buckets, now, 1)
			}
		})
	})
}

func BenchmarkLimiter_AllowWithDebug(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string { return "allow-debug-single-bucket" }

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.allowNWithDebug(0, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyFunc, limit1, limit2)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.allowNWithDebug(0, now, 1)
			}
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyFunc := func(id int) int { return id }

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.allowNWithDebug(i%buckets, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyFunc, limit1, limit2)
			now := ntime.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.allowNWithDebug(i%buckets, now, 1)
			}
		})
	})
}
