package rate

import (
	"strconv"
	"testing"
	"time"
)

// BenchmarkLimiter_AllowN_MultipleBuckets_SingleLimit benchmarks allowN with multiple buckets and single limit (happy path)
func BenchmarkLimiter_AllowN_MultipleBuckets_SingleLimit(b *testing.B) {
	// Pre-compute bucket keys to avoid string allocation overhead during benchmark
	const buckets = 1000

	keyer := func(id int) int {
		return id
	}
	limit := NewLimit(10000000, time.Second) // Large limit to ensure all requests succeed
	limiter := NewLimiter(keyer, limit)

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()

	for b.Loop() {
		for i := range buckets {
			limiter.AllowN(i, 1)
		}
	}

	elapsed := time.Since(start)
	n := float64(b.N)

	nsPerOp := float64(elapsed.Nanoseconds()) / n / buckets
	b.ReportMetric(nsPerOp, "ns/allow")
}

// BenchmarkLimiter_SingleVsMultiLimit compares performance between single and multi-limit scenarios
func BenchmarkLimiter_SingleVsMultiLimit(b *testing.B) {
	const buckets = 1000
	keys := make([]string, buckets)
	for i := range buckets {
		keys[i] = "bucket-" + strconv.Itoa(i)
	}

	keyer := func(id int) string {
		return keys[id]
	}

	b.Run("SingleLimit", func(b *testing.B) {
		limit := NewLimit(1000000, time.Second)
		limiter := NewLimiter(keyer, limit)
		now := time.Now()

		// Warm up buckets
		for i := range buckets {
			limiter.allowN(i, now, 1)
		}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			for j := range buckets {
				limiter.allowN(j, now, 1)
			}
		}
	})

	b.Run("MultiLimit", func(b *testing.B) {
		limit1 := NewLimit(1000000, time.Second)
		limit2 := NewLimit(500000, time.Second/2)
		limiter := NewLimiter(keyer, limit1, limit2)
		now := time.Now()

		// Warm up buckets
		for i := range buckets {
			limiter.allowN(i, now, 1)
		}

		b.ResetTimer()
		b.ReportAllocs()

		for b.Loop() {
			for j := range buckets {
				limiter.allowN(j, now, 1)
			}
		}
	})
}

// BenchmarkLimiter_SingleBucket tests single bucket access patterns (common for per-endpoint rate limiting)
func BenchmarkLimiter_SingleBucket(b *testing.B) {
	keyer := func(id int) string {
		return "single-bucket"
	}
	limit := NewLimit(1000000, time.Second)
	limiter := NewLimiter(keyer, limit)
	now := time.Now()

	b.ReportAllocs()

	for b.Loop() {
		limiter.allowN(0, now, 1)
	}
}

// BenchmarkLimiter_LargeBucketCount tests behavior with many buckets (realistic for production)
func BenchmarkLimiter_LargeBucketCount(b *testing.B) {
	const buckets = 100000 // Large bucket count simulating many users/IPs
	keys := make([]string, buckets)
	for i := range buckets {
		keys[i] = "bucket-" + strconv.Itoa(i)
	}

	keyer := func(id int) string {
		return keys[id]
	}
	limit := NewLimit(1000000, time.Second)
	limiter := NewLimiter(keyer, limit)
	now := time.Now()

	// Only warm up first 1000 buckets to test mixed hot/cold access
	for i := 0; i < 1000; i++ {
		limiter.allowN(i, now, 1)
	}

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		// Access pattern: mostly hot buckets with occasional cold ones
		bucketID := i % buckets
		limiter.allowN(bucketID, now, 1)
	}
}

// BenchmarkLimiter_AllowWithDetails_Fast compares the new fast API vs debug API
func BenchmarkLimiter_AllowWithDetailsAndDebug(b *testing.B) {
	keyer := func(input string) string { return input }
	perSecond := NewLimit(10000000, time.Second) // Large limit to avoid denials
	perMinute := NewLimit(10000000, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)

	b.Run("AllowWithDetails", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			limiter.AllowWithDetails("benchmark-key")
		}
	})

	b.Run("AllowWithDebug", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			limiter.AllowWithDebug("benchmark-key")
		}
	})
}

// BenchmarkLimiter_PeekWithDetails_Fast compares the new fast peek API vs debug API
func BenchmarkLimiter_PeekWithDetailsAndDebug(b *testing.B) {
	keyer := func(input string) string { return input }
	perSecond := NewLimit(10000000, time.Second)
	perMinute := NewLimit(10000000, time.Minute)
	limiter := NewLimiter(keyer, perSecond, perMinute)

	b.Run("PeekWithDetails", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			limiter.PeekWithDetails("benchmark-key")
		}
	})

	b.Run("PeekWithDebug", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			limiter.PeekWithDebug("benchmark-key")
		}
	})
}
