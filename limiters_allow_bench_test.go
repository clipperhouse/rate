package rate

import (
	"testing"
	"time"
)

// Hierarchical benchmarks for Limiters.allowN providing clearer grouping.
// Layout:
// BenchmarkLimiters_Allow/
//
//	SingleLimiter/{SingleBucket,MultipleBuckets}/{SingleLimit,MultipleLimits}
//	MultipleLimiters/{SameKeyer,DifferentKeyers}/{SingleBucket,MultipleBuckets}/{SingleLimit,MultipleLimits}
func BenchmarkLimiters_Allow(b *testing.B) {
	b.Run("SingleLimiter", func(b *testing.B) {
		// Same key for single bucket scenario
		keyerSingleBucket := func(_ string) string { return "single-limiter-single-bucket" }

		b.Run("SingleBucket", func(b *testing.B) {
			b.Run("SingleLimit", func(b *testing.B) {
				limit := NewLimit(1_000_000, time.Second)
				limiter := NewLimiter(keyerSingleBucket, limit)
				limiters := NewLimiters(limiter)
				now := time.Now()
				b.ReportAllocs()
				for b.Loop() {
					limiters.allowN("x", now, 1)
				}
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyerSingleBucket, limit1, limit2)
				limiters := NewLimiters(limiter)
				now := time.Now()
				b.ReportAllocs()
				for b.Loop() {
					limiters.allowN("x", now, 1)
				}
			})
		})

		b.Run("MultipleBuckets", func(b *testing.B) {
			const buckets = 1000
			keyer := func(id int) int { return id }
			b.Run("SingleLimit", func(b *testing.B) {
				limit := NewLimit(1_000_000, time.Second)
				limiter := NewLimiter(keyer, limit)
				limiters := NewLimiters(limiter)
				now := time.Now()
				b.ResetTimer()
				b.ReportAllocs()
				i := 0
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiters.allowN(i%buckets, now, 1)
						i++
					}
				})
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyer, limit1, limit2)
				limiters := NewLimiters(limiter)
				now := time.Now()
				b.ResetTimer()
				b.ReportAllocs()
				i := 0
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						limiters.allowN(i%buckets, now, 1)
						i++
					}
				})
			})
		})
	})

	b.Run("MultipleLimiters", func(b *testing.B) {
		b.Run("SameKeyer", func(b *testing.B) {
			keyerStr := func(_ string) string { return "multi-limiters-same-keyer-single-bucket" }
			b.Run("SingleBucket", func(b *testing.B) {
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyerStr, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyerStr, NewLimit(1_000_000, time.Second))
					limiters := NewLimiters(l1, l2)
					now := time.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowN("x", now, 1)
					}
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyerStr, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyerStr, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := NewLimiters(l1, l2)
					now := time.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowN("x", now, 1)
					}
				})
			})
			b.Run("MultipleBuckets", func(b *testing.B) {
				const buckets = 1000
				keyer := func(id int) int { return id }
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyer, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyer, NewLimit(1_000_000, time.Second))
					limiters := NewLimiters(l1, l2)
					now := time.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := 0
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							limiters.allowN(i%buckets, now, 1)
							i++
						}
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyer, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyer, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := NewLimiters(l1, l2)
					now := time.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := 0
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							limiters.allowN(i%buckets, now, 1)
							i++
						}
					})
				})
			})
		})

		b.Run("DifferentKeyers", func(b *testing.B) {
			keyer1Str := func(s string) string { return "limiter1-" + s }
			keyer2Str := func(s string) string { return "limiter2-" + s }
			b.Run("SingleBucket", func(b *testing.B) {
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyer1Str, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyer2Str, NewLimit(1_000_000, time.Second))
					limiters := NewLimiters(l1, l2)
					now := time.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowN("x", now, 1)
					}
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyer1Str, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyer2Str, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := NewLimiters(l1, l2)
					now := time.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowN("x", now, 1)
					}
				})
			})
			b.Run("MultipleBuckets", func(b *testing.B) {
				const buckets = 1000
				keyer1 := func(id int) int { return id }
				keyer2 := func(id int) int { return id * 131 }
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyer1, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyer2, NewLimit(1_000_000, time.Second))
					limiters := NewLimiters(l1, l2)
					now := time.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := 0
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							limiters.allowN(i%buckets, now, 1)
							i++
						}
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyer1, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyer2, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := NewLimiters(l1, l2)
					now := time.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := 0
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							limiters.allowN(i%buckets, now, 1)
							i++
						}
					})
				})
			})
		})
	})
}
