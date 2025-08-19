package rate

import (
	"testing"
	"time"

	"github.com/clipperhouse/ntime"
)

func BenchmarkLimiter_Peek(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string {
			return "peek-single-bucket"
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.peekN(0, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyFunc, limit1, limit2)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.peekN(0, now, 1)
			}
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyFunc := func(id int) int {
			return id
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.peekN(i%buckets, now, 1)
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
				limiter.peekN(i%buckets, now, 1)
			}
		})
	})
}

func BenchmarkLimiter_PeekWithDetails(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string {
			return "peek-single-bucket"
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.peekNWithDetails(0, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyFunc, limit1, limit2)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.peekNWithDetails(0, now, 1)
			}
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyFunc := func(id int) int {
			return id
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.peekNWithDetails(i%buckets, now, 1)
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
				limiter.peekNWithDetails(i%buckets, now, 1)
			}
		})
	})
}

func BenchmarkLimiter_PeekWithDebug(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyFunc := func(id int) string {
			return "peek-debug-single-bucket"
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.peekNWithDebug(0, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyFunc, limit1, limit2)
			now := ntime.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.peekNWithDebug(0, now, 1)
			}
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyFunc := func(id int) int {
			return id
		}

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyFunc, limit)
			now := ntime.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.peekNWithDebug(i%buckets, now, 1)
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
				limiter.peekNWithDebug(i%buckets, now, 1)
			}
		})
	})
}
