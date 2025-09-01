package rate

import (
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkLimiters_PeekN provides benchmarks for the new PeekN functionality
func BenchmarkLimiters_PeekN(b *testing.B) {
	b.Run("SingleLimiter", func(b *testing.B) {
		b.Run("SingleBucket", func(b *testing.B) {
			keyFunc := func(_ string) string { return "single-limiter-single-bucket" }
			b.Run("SingleLimit", func(b *testing.B) {
				limit := NewLimit(1_000_000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				limiters := Combine(limiter)
				b.ReportAllocs()
				for b.Loop() {
					limiters.PeekN("x", 1)
				}
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				limiters := Combine(limiter)
				b.ReportAllocs()
				for b.Loop() {
					limiters.PeekN("x", 1)
				}
			})
		})

		b.Run("MultipleBuckets", func(b *testing.B) {
			const buckets = int64(1000)
			keyFunc := func(id int64) int64 { return id }
			b.Run("SingleLimit", func(b *testing.B) {
				limit := NewLimit(1_000_000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				limiters := Combine(limiter)
				b.ResetTimer()
				b.ReportAllocs()
				i := int64(0)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						idx := atomic.AddInt64(&i, 1)
						limiters.PeekN(idx%buckets, 1)
					}
				})
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				limiters := Combine(limiter)
				b.ResetTimer()
				b.ReportAllocs()
				i := int64(0)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						idx := atomic.AddInt64(&i, 1)
						limiters.PeekN(idx%buckets, 1)
					}
				})
			})
		})
	})

	b.Run("MultipleLimiters", func(b *testing.B) {
		b.Run("SameKeyer", func(b *testing.B) {
			keyFunc := func(_ string) string { return "multi-limiters-same-keyFunc-single-bucket" }
			b.Run("SingleBucket", func(b *testing.B) {
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
					limiters := Combine(l1, l2)
					b.ReportAllocs()
					for b.Loop() {
						limiters.PeekN("x", 1)
					}
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					b.ReportAllocs()
					for b.Loop() {
						limiters.PeekN("x", 1)
					}
				})
			})
			b.Run("MultipleBuckets", func(b *testing.B) {
				const buckets = int64(1000)
				keyFunc := func(id int64) int64 { return id }
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
					limiters := Combine(l1, l2)
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.PeekN(idx%buckets, 1)
						}
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.PeekN(idx%buckets, 1)
						}
					})
				})
			})
		})
	})
}

// BenchmarkLimiters_PeekNWithDetails provides benchmarks for the new PeekNWithDetails functionality
func BenchmarkLimiters_PeekNWithDetails(b *testing.B) {
	b.Run("SingleLimiter", func(b *testing.B) {
		b.Run("SingleBucket", func(b *testing.B) {
			keyFunc := func(_ string) string { return "single-limiter-single-bucket" }
			b.Run("SingleLimit", func(b *testing.B) {
				limit := NewLimit(1_000_000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				limiters := Combine(limiter)
				b.ReportAllocs()
				for b.Loop() {
					limiters.PeekNWithDetails("x", 1)
				}
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				limiters := Combine(limiter)
				b.ReportAllocs()
				for b.Loop() {
					limiters.PeekNWithDetails("x", 1)
				}
			})
		})

		b.Run("MultipleBuckets", func(b *testing.B) {
			const buckets = int64(1000)
			keyFunc := func(id int64) int64 { return id }
			b.Run("SingleLimit", func(b *testing.B) {
				limit := NewLimit(1_000_000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				limiters := Combine(limiter)
				b.ResetTimer()
				b.ReportAllocs()
				i := int64(0)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						idx := atomic.AddInt64(&i, 1)
						limiters.PeekNWithDetails(idx%buckets, 1)
					}
				})
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				limiters := Combine(limiter)
				b.ResetTimer()
				b.ReportAllocs()
				i := int64(0)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						idx := atomic.AddInt64(&i, 1)
						limiters.PeekNWithDetails(idx%buckets, 1)
					}
				})
			})
		})
	})

	b.Run("MultipleLimiters", func(b *testing.B) {
		b.Run("SameKeyer", func(b *testing.B) {
			keyFunc := func(_ string) string { return "multi-limiters-same-keyFunc-single-bucket" }
			b.Run("SingleBucket", func(b *testing.B) {
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
					limiters := Combine(l1, l2)
					b.ReportAllocs()
					for b.Loop() {
						limiters.PeekNWithDetails("x", 1)
					}
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					b.ReportAllocs()
					for b.Loop() {
						limiters.PeekNWithDetails("x", 1)
					}
				})
			})
			b.Run("MultipleBuckets", func(b *testing.B) {
				const buckets = int64(1000)
				keyFunc := func(id int64) int64 { return id }
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
					limiters := Combine(l1, l2)
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.PeekNWithDetails(idx%buckets, 1)
						}
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.PeekNWithDetails(idx%buckets, 1)
						}
					})
				})
			})
		})
	})
}
