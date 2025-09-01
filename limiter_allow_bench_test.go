package rate

import (
	"testing"
	"time"
)

func BenchmarkLimiter_Allow(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string { return "Allow-single-bucket" }

		b.Run("SingleLimit", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				b.ReportAllocs()

				for b.Loop() {
					limiter.AllowN(0, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.AllowN(0, 1)
					}
				})
			})
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)

				b.ReportAllocs()

				for b.Loop() {
					limiter.AllowN(0, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.AllowN(0, 1)
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

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; b.Loop(); i++ {
					limiter.AllowN(i%buckets, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.AllowN(i%buckets, 1)
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

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; b.Loop(); i++ {
					limiter.AllowN(i%buckets, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.AllowN(i%buckets, 1)
						i++
					}
				})
			})
		})
	})
}

func BenchmarkLimiter_AllowWithDetails(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string { return "Allow-details-single-bucket" }

		b.Run("SingleLimit", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				b.ReportAllocs()

				for b.Loop() {
					limiter.AllowNWithDetails(0, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.AllowNWithDetails(0, 1)
					}
				})
			})
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)

				b.ReportAllocs()

				for b.Loop() {
					limiter.AllowNWithDetails(0, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.AllowNWithDetails(0, 1)
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

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					limiter.AllowNWithDetails(i%buckets, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.AllowNWithDetails(i%buckets, 1)
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

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					limiter.AllowNWithDetails(i%buckets, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.AllowNWithDetails(i%buckets, 1)
						i++
					}
				})
			})
		})
	})
}

func BenchmarkLimiter_AllowWithDebug(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string { return "Allow-debug-single-bucket" }

		b.Run("SingleLimit", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				b.ReportAllocs()

				for b.Loop() {
					limiter.AllowNWithDebug(0, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.AllowNWithDebug(0, 1)
					}
				})
			})
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			b.Run("Serial", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)

				b.ReportAllocs()

				for b.Loop() {
					limiter.AllowNWithDebug(0, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)

				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiter.AllowNWithDebug(0, 1)
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

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					limiter.AllowNWithDebug(i%buckets, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit := NewLimit(1000000, time.Second)
				limiter := NewLimiter(keyFunc, limit)

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.AllowNWithDebug(i%buckets, 1)
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

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					limiter.AllowNWithDebug(i%buckets, 1)
				}
			})
			b.Run("Parallel", func(b *testing.B) {
				limit1 := NewLimit(1000000, time.Second)
				limit2 := NewLimit(500000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)

				b.ResetTimer()
				b.ReportAllocs()

				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						limiter.AllowNWithDebug(i%buckets, 1)
						i++
					}
				})
			})
		})
	})
}
