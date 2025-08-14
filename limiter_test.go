package rate

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLimiter_GetLimits(t *testing.T) {
	t.Parallel()

	t.Run("StaticLimits_ReturnsDirectly", func(t *testing.T) {
		t.Parallel()

		// Test with single static limit
		limit1 := NewLimit(10, time.Second)
		limiter := NewLimiter(func(s string) string { return s }, limit1)

		limits := limiter.getLimits("test")
		require.Len(t, limits, 1, "should return exactly one limit")
		require.Equal(t, limit1, limits[0], "should return the exact limit provided")

		// Test with multiple static limits
		limit2 := NewLimit(5, time.Minute)
		limit3 := NewLimit(100, time.Hour)
		limiter = NewLimiter(func(s string) string { return s }, limit1, limit2, limit3)

		limits = limiter.getLimits("test")
		require.Len(t, limits, 3, "should return exactly three limits")
		require.Equal(t, limit1, limits[0], "first limit should match")
		require.Equal(t, limit2, limits[1], "second limit should match")
		require.Equal(t, limit3, limits[2], "third limit should match")
	})

	t.Run("DynamicLimits_CallsFunctions", func(t *testing.T) {
		t.Parallel()

		// Test with single dynamic limit function
		limitFunc1 := func(input string) Limit {
			if input == "high" {
				return NewLimit(100, time.Second)
			}
			return NewLimit(10, time.Second)
		}

		limiter := NewLimiterFunc(func(s string) string { return s }, limitFunc1)

		limits := limiter.getLimits("high")
		require.Len(t, limits, 1, "should return exactly one limit")
		require.Equal(t, int64(100), limits[0].Count(), "should call function with input 'high'")
		require.Equal(t, time.Second, limits[0].Period(), "should have correct period")

		limits = limiter.getLimits("low")
		require.Len(t, limits, 1, "should return exactly one limit")
		require.Equal(t, int64(10), limits[0].Count(), "should call function with input 'low'")
		require.Equal(t, time.Second, limits[0].Period(), "should have correct period")

		// Test with multiple dynamic limit functions
		limitFunc2 := func(input string) Limit {
			if input == "admin" {
				return NewLimit(1000, time.Minute)
			}
			return NewLimit(100, time.Minute)
		}

		limiter = NewLimiterFunc(func(s string) string { return s }, limitFunc1, limitFunc2)

		limits = limiter.getLimits("admin")
		require.Len(t, limits, 2, "should return exactly two limits")
		require.Equal(t, int64(10), limits[0].Count(), "first function should be called with 'admin'")
		require.Equal(t, int64(1000), limits[1].Count(), "second function should be called with 'admin'")
	})

	t.Run("MixedInputTypes", func(t *testing.T) {
		t.Parallel()

		// Test with different input types
		type User struct {
			ID   int
			Role string
		}

		limitFunc := func(user User) Limit {
			if user.Role == "admin" {
				return NewLimit(1000, time.Minute)
			}
			return NewLimit(100, time.Minute)
		}

		limiter := NewLimiterFunc(func(user User) int { return user.ID }, limitFunc)

		adminUser := User{ID: 1, Role: "admin"}
		regularUser := User{ID: 2, Role: "user"}

		adminLimits := limiter.getLimits(adminUser)
		require.Len(t, adminLimits, 1, "should return one limit for admin user")
		require.Equal(t, int64(1000), adminLimits[0].Count(), "admin should get high limit")

		regularLimits := limiter.getLimits(regularUser)
		require.Len(t, regularLimits, 1, "should return one limit for regular user")
		require.Equal(t, int64(100), regularLimits[0].Count(), "regular user should get low limit")
	})

	t.Run("EmptyLimits_ReturnsEmptySlice", func(t *testing.T) {
		t.Parallel()

		// Test with no static limits and no dynamic functions
		limiter := NewLimiter(func(s string) string { return s })

		limits := limiter.getLimits("test")
		require.Len(t, limits, 0, "should return empty slice when no limits configured")

		// Test with no dynamic functions
		limiter = NewLimiterFunc(func(s string) string { return s })

		limits = limiter.getLimits("test")
		require.Len(t, limits, 0, "should return empty slice when no limit functions configured")
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		t.Parallel()

		limit := NewLimit(10, time.Second)
		limiter := NewLimiter(func(s string) string { return s }, limit)

		const concurrency = 100
		const calls = 1000

		var wg sync.WaitGroup
		wg.Add(concurrency)
		for range concurrency {
			go func() {
				defer wg.Done()
				for range calls {
					limits := limiter.getLimits("test")
					require.Len(t, limits, 1, "should always return one limit")
					require.Equal(t, limit, limits[0], "should always return the same limit")
				}
			}()
		}
		wg.Wait()
	})

	t.Run("NilInput_HandlesGracefully", func(t *testing.T) {
		t.Parallel()

		// Test with nil input for dynamic limits
		limitFunc := func(input *string) Limit {
			if input == nil {
				return NewLimit(1, time.Second) // Use 1 instead of 0 to avoid division by zero
			}
			return NewLimit(10, time.Second)
		}

		limiter := NewLimiterFunc(func(s *string) string {
			if s == nil {
				return "nil"
			}
			return *s
		}, limitFunc)

		limits := limiter.getLimits(nil)
		require.Len(t, limits, 1, "should handle nil input gracefully")
		require.Equal(t, int64(1), limits[0].Count(), "should call function with nil input")
	})

	t.Run("ComplexInputTypes", func(t *testing.T) {
		t.Parallel()

		type Request struct {
			Method string
			Path   string
			UserID int
		}

		limitFunc := func(req Request) Limit {
			switch {
			case req.Method == "GET" && req.Path == "/api/users":
				return NewLimit(1000, time.Minute)
			case req.Method == "POST" && req.Path == "/api/users":
				return NewLimit(100, time.Minute)
			case req.UserID == 1: // admin user
				return NewLimit(500, time.Minute)
			default:
				return NewLimit(50, time.Minute)
			}
		}

		limiter := NewLimiterFunc(func(req Request) string {
			return req.Method + ":" + req.Path
		}, limitFunc)

		testCases := []struct {
			name     string
			request  Request
			expected int64
		}{
			{"GET users", Request{Method: "GET", Path: "/api/users", UserID: 2}, 1000},
			{"POST users", Request{Method: "POST", Path: "/api/users", UserID: 2}, 100},
			{"Admin user", Request{Method: "PUT", Path: "/api/other", UserID: 1}, 500},
			{"Regular user", Request{Method: "GET", Path: "/api/other", UserID: 2}, 50},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				limits := limiter.getLimits(tc.request)
				require.Len(t, limits, 1, "should return one limit")
				require.Equal(t, tc.expected, limits[0].Count(),
					"expected limit %d for request %+v", tc.expected, tc.request)
			})
		}
	})
}
