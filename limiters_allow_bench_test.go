package rate

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/clipperhouse/ntime"
)

// Hierarchical benchmarks for Limiters.allowN providing clearer grouping.
// Layout:
// BenchmarkLimiters_Allow/
//
//	SingleLimiter/{SingleBucket,MultipleBuckets}/{SingleLimit,MultipleLimits}/{Serial,Parallel}
//	MultipleLimiters/{SameKeyer,DifferentKeyers}/{SingleBucket,MultipleBuckets}/{SingleLimit,MultipleLimits}/{Serial,Parallel}
func BenchmarkLimiters_Allow(b *testing.B) {
	b.Run("SingleLimiter", func(b *testing.B) {
		// Same key for single bucket scenario

		b.Run("SingleBucket", func(b *testing.B) {
			keyFunc := func(_ string) string { return "single-limiter-single-bucket" }
			b.Run("SingleLimit", func(b *testing.B) {
				b.Run("Serial", func(b *testing.B) {
					limit := NewLimit(1_000_000, time.Second)
					limiter := NewLimiter(keyFunc, limit)
					limiters := Combine(limiter)
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowN("x", now, 1)
					}
				})
				b.Run("Parallel", func(b *testing.B) {
					limit := NewLimit(1_000_000, time.Second)
					limiter := NewLimiter(keyFunc, limit)
					limiters := Combine(limiter)
					now := ntime.Now()
					b.ReportAllocs()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							limiters.allowN("x", now, 1)
						}
					})
				})
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				b.Run("Serial", func(b *testing.B) {
					limit1 := NewLimit(1_000_000, time.Second)
					limit2 := NewLimit(500_000, time.Second/2)
					limiter := NewLimiter(keyFunc, limit1, limit2)
					limiters := Combine(limiter)
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowN("x", now, 1)
					}
				})
				b.Run("Parallel", func(b *testing.B) {
					limit1 := NewLimit(1_000_000, time.Second)
					limit2 := NewLimit(500_000, time.Second/2)
					limiter := NewLimiter(keyFunc, limit1, limit2)
					limiters := Combine(limiter)
					now := ntime.Now()
					b.ReportAllocs()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							limiters.allowN("x", now, 1)
						}
					})
				})
			})
		})

		b.Run("MultipleBuckets", func(b *testing.B) {
			const buckets = int64(1000)
			keyFunc := func(id int64) int64 { return id }
			b.Run("SingleLimit", func(b *testing.B) {
				b.Run("Serial", func(b *testing.B) {
					limit := NewLimit(1_000_000, time.Second)
					limiter := NewLimiter(keyFunc, limit)
					limiters := Combine(limiter)
					now := ntime.Now()
					b.ReportAllocs()
					i := int64(0)
					for b.Loop() {
						idx := atomic.AddInt64(&i, 1)
						limiters.allowN(idx%buckets, now, 1)
					}
				})
				b.Run("Parallel", func(b *testing.B) {
					limit := NewLimit(1_000_000, time.Second)
					limiter := NewLimiter(keyFunc, limit)
					limiters := Combine(limiter)
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowN(idx%buckets, now, 1)
						}
					})
				})
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				b.Run("Serial", func(b *testing.B) {
					limit1 := NewLimit(1_000_000, time.Second)
					limit2 := NewLimit(500_000, time.Second/2)
					limiter := NewLimiter(keyFunc, limit1, limit2)
					limiters := Combine(limiter)
					now := ntime.Now()
					b.ReportAllocs()
					i := int64(0)
					for b.Loop() {
						idx := atomic.AddInt64(&i, 1)
						limiters.allowN(idx%buckets, now, 1)
					}
				})
				b.Run("Parallel", func(b *testing.B) {
					limit1 := NewLimit(1_000_000, time.Second)
					limit2 := NewLimit(500_000, time.Second/2)
					limiter := NewLimiter(keyFunc, limit1, limit2)
					limiters := Combine(limiter)
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowN(idx%buckets, now, 1)
						}
					})
				})
			})
		})
	})

	b.Run("MultipleLimiters", func(b *testing.B) {
		b.Run("SameKeyer", func(b *testing.B) {
			keyFunc := func(_ string) string { return "multi-limiters-same-keyFunc-single-bucket" }
			b.Run("SingleBucket", func(b *testing.B) {
				b.Run("SingleLimit", func(b *testing.B) {
					b.Run("Serial", func(b *testing.B) {
						l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
						l2 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						for b.Loop() {
							limiters.allowN("x", now, 1)
						}
					})
					b.Run("Parallel", func(b *testing.B) {
						l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
						l2 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								limiters.allowN("x", now, 1)
							}
						})
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					b.Run("Serial", func(b *testing.B) {
						l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
						l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						for b.Loop() {
							limiters.allowN("x", now, 1)
						}
					})
					b.Run("Parallel", func(b *testing.B) {
						l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
						l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								limiters.allowN("x", now, 1)
							}
						})
					})
				})
			})
			b.Run("MultipleBuckets", func(b *testing.B) {
				const buckets = int64(1000)
				keyFunc := func(id int64) int64 { return id }
				b.Run("SingleLimit", func(b *testing.B) {
					b.Run("Serial", func(b *testing.B) {
						l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
						l2 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						i := int64(0)
						for b.Loop() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowN(idx%buckets, now, 1)
						}
					})
					b.Run("Parallel", func(b *testing.B) {
						l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
						l2 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ResetTimer()
						b.ReportAllocs()
						i := int64(0)
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								idx := atomic.AddInt64(&i, 1)
								limiters.allowN(idx%buckets, now, 1)
							}
						})
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					b.Run("Serial", func(b *testing.B) {
						l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
						l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						i := int64(0)
						for b.Loop() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowN(idx%buckets, now, 1)
						}
					})
					b.Run("Parallel", func(b *testing.B) {
						l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
						l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ResetTimer()
						b.ReportAllocs()
						i := int64(0)
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								idx := atomic.AddInt64(&i, 1)
								limiters.allowN(idx%buckets, now, 1)
							}
						})
					})
				})
			})
		})

		b.Run("DifferentKeyers", func(b *testing.B) {
			keyFunc1 := func(_ string) int64 { return 1 }
			keyFunc2 := func(_ string) int64 { return 2 }
			b.Run("SingleBucket", func(b *testing.B) {
				b.Run("SingleLimit", func(b *testing.B) {
					b.Run("Serial", func(b *testing.B) {
						l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second))
						l2 := NewLimiter(keyFunc2, NewLimit(1_000_000, time.Second))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						for b.Loop() {
							limiters.allowN("x", now, 1)
						}
					})
					b.Run("Parallel", func(b *testing.B) {
						l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second))
						l2 := NewLimiter(keyFunc2, NewLimit(1_000_000, time.Second))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								limiters.allowN("x", now, 1)
							}
						})
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					b.Run("Serial", func(b *testing.B) {
						l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
						l2 := NewLimiter(keyFunc2, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						for b.Loop() {
							limiters.allowN("x", now, 1)
						}
					})
					b.Run("Parallel", func(b *testing.B) {
						l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
						l2 := NewLimiter(keyFunc2, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								limiters.allowN("x", now, 1)
							}
						})
					})
				})
			})
			b.Run("MultipleBuckets", func(b *testing.B) {
				const buckets = int64(1000)
				keyFunc1 := func(id int64) int64 { return id }
				keyFunc2 := func(id int64) int64 { return id * 131 }
				b.Run("SingleLimit", func(b *testing.B) {
					b.Run("Serial", func(b *testing.B) {
						l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second))
						l2 := NewLimiter(keyFunc2, NewLimit(1_000_000, time.Second))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						i := int64(0)
						for b.Loop() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowN(idx%buckets, now, 1)
						}
					})
					b.Run("Parallel", func(b *testing.B) {
						l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second))
						l2 := NewLimiter(keyFunc2, NewLimit(1_000_000, time.Second))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ResetTimer()
						b.ReportAllocs()
						i := int64(0)
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								idx := atomic.AddInt64(&i, 1)
								limiters.allowN(idx%buckets, now, 1)
							}
						})
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					b.Run("Serial", func(b *testing.B) {
						l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
						l2 := NewLimiter(keyFunc2, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ReportAllocs()
						i := int64(0)
						for b.Loop() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowN(idx%buckets, now, 1)
						}
					})
					b.Run("Parallel", func(b *testing.B) {
						l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
						l2 := NewLimiter(keyFunc2, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
						limiters := Combine(l1, l2)
						now := ntime.Now()
						b.ResetTimer()
						b.ReportAllocs()
						i := int64(0)
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								idx := atomic.AddInt64(&i, 1)
								limiters.allowN(idx%buckets, now, 1)
							}
						})
					})
				})
			})
		})
	})
}

// Hierarchical benchmarks for Limiters.allowNWithDetails providing clearer grouping.
// Layout matches the Allow benchmarks for easy comparison.
func BenchmarkLimiters_AllowWithDetails(b *testing.B) {
	b.Run("SingleLimiter", func(b *testing.B) {
		b.Run("SingleBucket", func(b *testing.B) {
			keyFunc := func(_ string) string { return "single-limiter-single-bucket" }
			b.Run("SingleLimit", func(b *testing.B) {
				limit := NewLimit(1_000_000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				limiters := Combine(limiter)
				now := ntime.Now()
				b.ReportAllocs()
				for b.Loop() {
					limiters.allowNWithDetails("x", now, 1)
				}
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				limiters := Combine(limiter)
				now := ntime.Now()
				b.ReportAllocs()
				for b.Loop() {
					limiters.allowNWithDetails("x", now, 1)
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
				now := ntime.Now()
				b.ResetTimer()
				b.ReportAllocs()
				i := int64(0)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						idx := atomic.AddInt64(&i, 1)
						limiters.allowNWithDetails(idx%buckets, now, 1)
					}
				})
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				limiters := Combine(limiter)
				now := ntime.Now()
				b.ResetTimer()
				b.ReportAllocs()
				i := int64(0)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						idx := atomic.AddInt64(&i, 1)
						limiters.allowNWithDetails(idx%buckets, now, 1)
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
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowNWithDetails("x", now, 1)
					}
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowNWithDetails("x", now, 1)
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
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowNWithDetails(idx%buckets, now, 1)
						}
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowNWithDetails(idx%buckets, now, 1)
						}
					})
				})
			})
		})
		b.Run("DifferentKeyers", func(b *testing.B) {
			b.Run("SingleBucket", func(b *testing.B) {
				keyFunc1 := func(_ string) int64 { return 1 }
				keyFunc2 := func(_ string) int64 { return 2 }
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyFunc2, NewLimit(1_000_000, time.Second))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowNWithDetails("x", now, 1)
					}
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc2, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowNWithDetails("x", now, 1)
					}
				})
			})
			b.Run("MultipleBuckets", func(b *testing.B) {
				const buckets = int64(1000)
				keyFunc1 := func(id int64) int64 { return id }
				keyFunc2 := func(id int64) int64 { return id * 131 }
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyFunc2, NewLimit(1_000_000, time.Second))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowNWithDetails(idx%buckets, now, 1)
						}
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc2, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowNWithDetails(idx%buckets, now, 1)
						}
					})
				})
			})
		})
	})
}

// Hierarchical benchmarks for Limiters.allowNWithDebug providing clearer grouping.
// Layout matches the other benchmarks for easy comparison.
func BenchmarkLimiters_AllowWithDebug(b *testing.B) {
	b.Run("SingleLimiter", func(b *testing.B) {
		b.Run("SingleBucket", func(b *testing.B) {
			keyFunc := func(_ string) string { return "single-limiter-single-bucket" }
			b.Run("SingleLimit", func(b *testing.B) {
				limit := NewLimit(1_000_000, time.Second)
				limiter := NewLimiter(keyFunc, limit)
				limiters := Combine(limiter)
				now := ntime.Now()
				b.ReportAllocs()
				for b.Loop() {
					limiters.allowNWithDebug("x", now, 1)
				}
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				limiters := Combine(limiter)
				now := ntime.Now()
				b.ReportAllocs()
				for b.Loop() {
					limiters.allowNWithDebug("x", now, 1)
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
				now := ntime.Now()
				b.ResetTimer()
				b.ReportAllocs()
				i := int64(0)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						idx := atomic.AddInt64(&i, 1)
						limiters.allowNWithDebug(idx%buckets, now, 1)
					}
				})
			})
			b.Run("MultipleLimits", func(b *testing.B) {
				limit1 := NewLimit(1_000_000, time.Second)
				limit2 := NewLimit(500_000, time.Second/2)
				limiter := NewLimiter(keyFunc, limit1, limit2)
				limiters := Combine(limiter)
				now := ntime.Now()
				b.ResetTimer()
				b.ReportAllocs()
				i := int64(0)
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						idx := atomic.AddInt64(&i, 1)
						limiters.allowNWithDebug(idx%buckets, now, 1)
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
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowNWithDebug("x", now, 1)
					}
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowNWithDebug("x", now, 1)
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
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowNWithDebug(idx%buckets, now, 1)
						}
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowNWithDebug(idx%buckets, now, 1)
						}
					})
				})
			})
		})
		b.Run("DifferentKeyers", func(b *testing.B) {
			b.Run("SingleBucket", func(b *testing.B) {
				keyFunc1 := func(_ string) int64 { return 1 }
				keyFunc2 := func(_ string) int64 { return 2 }
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyFunc2, NewLimit(1_000_000, time.Second))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowNWithDebug("x", now, 1)
					}
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc2, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ReportAllocs()
					for b.Loop() {
						limiters.allowNWithDebug("x", now, 1)
					}
				})
			})
			b.Run("MultipleBuckets", func(b *testing.B) {
				const buckets = int64(1000)
				keyFunc1 := func(id int64) int64 { return id }
				keyFunc2 := func(id int64) int64 { return id * 131 }
				b.Run("SingleLimit", func(b *testing.B) {
					l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second))
					l2 := NewLimiter(keyFunc2, NewLimit(1_000_000, time.Second))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowNWithDebug(idx%buckets, now, 1)
						}
					})
				})
				b.Run("MultipleLimits", func(b *testing.B) {
					l1 := NewLimiter(keyFunc1, NewLimit(1_000_000, time.Second), NewLimit(500_000, time.Second/2))
					l2 := NewLimiter(keyFunc2, NewLimit(750_000, time.Second), NewLimit(400_000, time.Second/2))
					limiters := Combine(l1, l2)
					now := ntime.Now()
					b.ResetTimer()
					b.ReportAllocs()
					i := int64(0)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							idx := atomic.AddInt64(&i, 1)
							limiters.allowNWithDebug(idx%buckets, now, 1)
						}
					})
				})
			})
		})
	})
}
