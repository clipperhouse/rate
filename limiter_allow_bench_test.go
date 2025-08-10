package rate

import (
	"testing"
	"time"
)

func BenchmarkLimiter_Allow(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyer := func(id int) string { return "allow-single-bucket" }

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyer, limit)
			now := time.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.allowN(0, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyer, limit1, limit2)
			now := time.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.allowN(0, now, 1)
			}
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyer := func(id int) int { return id }

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyer, limit)
			now := time.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; b.Loop(); i++ {
				limiter.allowN(i%buckets, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyer, limit1, limit2)
			now := time.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; b.Loop(); i++ {
				limiter.allowN(i%buckets, now, 1)
			}
		})
	})
}

func BenchmarkLimiter_AllowWithDetails(b *testing.B) {
	b.Run("SingleBucket", func(b *testing.B) {
		keyer := func(id int) string { return "allow-details-single-bucket" }

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyer, limit)
			now := time.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.allowNWithDetails(0, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyer, limit1, limit2)
			now := time.Now()

			b.ReportAllocs()

			for b.Loop() {
				limiter.allowNWithDetails(0, now, 1)
			}
		})
	})

	b.Run("MultipleBuckets", func(b *testing.B) {
		const buckets = 1000
		keyer := func(id int) int { return id }

		b.Run("SingleLimit", func(b *testing.B) {
			limit := NewLimit(1000000, time.Second)
			limiter := NewLimiter(keyer, limit)
			now := time.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.allowNWithDetails(i%buckets, now, 1)
			}
		})

		b.Run("MultipleLimits", func(b *testing.B) {
			limit1 := NewLimit(1000000, time.Second)
			limit2 := NewLimit(500000, time.Second/2)
			limiter := NewLimiter(keyer, limit1, limit2)
			now := time.Now()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				limiter.allowNWithDetails(i%buckets, now, 1)
			}
		})
	})
}

func BenchmarkLimiter_AllowWithDebug_SingleBucket(b *testing.B) {
	keyer := func(id int) string {
		return "allow-debug-single-bucket"
	}

	b.Run("SingleLimit", func(b *testing.B) {
		limit := NewLimit(1000000, time.Second)
		limiter := NewLimiter(keyer, limit)
		now := time.Now()

		b.ReportAllocs()

		for b.Loop() {
			limiter.allowNWithDebug(0, now, 1)
		}
	})

	b.Run("MultipleLimits", func(b *testing.B) {
		limit1 := NewLimit(1000000, time.Second)
		limit2 := NewLimit(500000, time.Second/2)
		limiter := NewLimiter(keyer, limit1, limit2)
		now := time.Now()

		b.ReportAllocs()

		for b.Loop() {
			limiter.allowNWithDebug(0, now, 1)
		}
	})
}

func BenchmarkLimiter_AllowWithDebug_MultipleBuckets(b *testing.B) {
	const buckets = 1000
	keyer := func(id int) int {
		return id
	}

	b.Run("SingleLimit", func(b *testing.B) {
		limit := NewLimit(1000000, time.Second)
		limiter := NewLimiter(keyer, limit)
		now := time.Now()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			limiter.allowNWithDebug(i%buckets, now, 1)
		}
	})

	b.Run("MultipleLimits", func(b *testing.B) {
		limit1 := NewLimit(1000000, time.Second)
		limit2 := NewLimit(500000, time.Second/2)
		limiter := NewLimiter(keyer, limit1, limit2)
		now := time.Now()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			limiter.allowNWithDebug(i%buckets, now, 1)
		}
	})
}
