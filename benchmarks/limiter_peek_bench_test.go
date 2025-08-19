package rate

import (
	"testing"
	"time"

	"github.com/clipperhouse/rate"
)

func BenchmarkLimiter_Peek(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string {
			return "Peek-single-bucket"
		}

		b.Run("SingleLimit", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit := rate.NewLimit(1000000, time.Second)
				limiter := rate.NewLimiter(keyFunc, limit)

				b.ReportAllocs()

				for b.Loop() {
					limiter.PeekN(0, 1)
				}
			})

			b.Run("Parallel", func(b *testing.B) {
				limit := rate.NewLimit(1000000, time.Second)
				limiter := rate.NewLimiter(keyFunc, limit)

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.PeekN(0, 1)
					}
				})
			})
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit1 := rate.NewLimit(1000000, time.Second)
				limit2 := rate.NewLimit(500000, time.Second/2)
				limiter := rate.NewLimiter(keyFunc, limit1, limit2)

				b.ReportAllocs()

				for b.Loop() {
					limiter.PeekN(0, 1)
				}
			})

			b.Run("Parallel", func(b *testing.B) {
				limit1 := rate.NewLimit(1000000, time.Second)
				limit2 := rate.NewLimit(500000, time.Second/2)
				limiter := rate.NewLimiter(keyFunc, limit1, limit2)

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.PeekN(0, 1)
					}
				})
			})
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyFunc := func(id int) int {
			return id
		}

		b.Run("SingleLimit", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit := rate.NewLimit(1000000, time.Second)
				limiter := rate.NewLimiter(keyFunc, limit)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					limiter.PeekN(i%buckets, 1)
				}
			})

			b.Run("Parallel", func(b *testing.B) {
				limit := rate.NewLimit(1000000, time.Second)
				limiter := rate.NewLimiter(keyFunc, limit)

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.PeekN(i%buckets, 1)
						i++
					}
				})
			})
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit1 := rate.NewLimit(1000000, time.Second)
				limit2 := rate.NewLimit(500000, time.Second/2)
				limiter := rate.NewLimiter(keyFunc, limit1, limit2)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					limiter.PeekN(i%buckets, 1)
				}
			})

			b.Run("Parallel", func(b *testing.B) {
				limit1 := rate.NewLimit(1000000, time.Second)
				limit2 := rate.NewLimit(500000, time.Second/2)
				limiter := rate.NewLimiter(keyFunc, limit1, limit2)

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.PeekN(i%buckets, 1)
						i++
					}
				})
			})
		})
	})
}

func BenchmarkLimiter_PeekWithDetails(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string {
			return "Peek-single-bucket"
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := rate.NewLimit(1000000, time.Second)
			limiter := rate.NewLimiter(keyFunc, limit)

			b.ReportAllocs()

			for b.Loop() {
				limiter.PeekNWithDetails(0, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := rate.NewLimit(1000000, time.Second)
			limit2 := rate.NewLimit(500000, time.Second/2)
			limiter := rate.NewLimiter(keyFunc, limit1, limit2)

			b.ReportAllocs()

			for b.Loop() {
				limiter.PeekNWithDetails(0, 1)
			}
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyFunc := func(id int) int {
			return id
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := rate.NewLimit(1000000, time.Second)
			limiter := rate.NewLimiter(keyFunc, limit)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.PeekNWithDetails(i%buckets, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := rate.NewLimit(1000000, time.Second)
			limit2 := rate.NewLimit(500000, time.Second/2)
			limiter := rate.NewLimiter(keyFunc, limit1, limit2)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.PeekNWithDetails(i%buckets, 1)
			}
		})
	})
}

func BenchmarkLimiter_PeekWithDebug(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string {
			return "Peek-debug-single-bucket"
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := rate.NewLimit(1000000, time.Second)
			limiter := rate.NewLimiter(keyFunc, limit)

			b.ReportAllocs()

			for b.Loop() {
				limiter.PeekNWithDebug(0, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := rate.NewLimit(1000000, time.Second)
			limit2 := rate.NewLimit(500000, time.Second/2)
			limiter := rate.NewLimiter(keyFunc, limit1, limit2)

			b.ReportAllocs()

			for b.Loop() {
				limiter.PeekNWithDebug(0, 1)
			}
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyFunc := func(id int) int {
			return id
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := rate.NewLimit(1000000, time.Second)
			limiter := rate.NewLimiter(keyFunc, limit)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.PeekNWithDebug(i%buckets, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := rate.NewLimit(1000000, time.Second)
			limit2 := rate.NewLimit(500000, time.Second/2)
			limiter := rate.NewLimiter(keyFunc, limit1, limit2)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.PeekNWithDebug(i%buckets, 1)
			}
		})
	})
}
