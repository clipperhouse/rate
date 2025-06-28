# Rate Limiter

Early days! I am designing a token bucket rate limiter with an emphasis on clean API and low overhead.

## Installation

```bash
go get github.com/clipperhouse/ratelimiter
```

[![Tests](https://github.com/clipperhouse/ratelimiter/actions/workflows/test.yml/badge.svg)](https://github.com/clipperhouse/ratelimiter/actions/workflows/test.yml)

## Sample code

```go
// Define a getter for the rate limiter "bucket"
func byIP(req *http.Request) string {
    // You can put arbitrary logic in here, perhaps by path or method,
    // or a combination thereof. In this case, we'll just use IP address.
    return req.RemoteAddr
}

// 10 requests per second
limit := ratelimiter.NewLimit(10, time.Second)

// 10 requests per second per IP
limiter := ratelimiter.NewRateLimiter(byIP, limit)

// In your HTTP handler
if limiter.Allow(r) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("Success"))
} else {
    w.WriteHeader(http.StatusTooManyRequests)
    w.Write([]byte("Too many requests"))
}

// Note that the limiter.Allow call is typed for http.Request, due to the signature of byIP
```
