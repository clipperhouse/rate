[![Tests](https://github.com/clipperhouse/rate/actions/workflows/tests.yml/badge.svg)](https://github.com/clipperhouse/rate/actions/workflows/tests.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/clipperhouse/rate.svg)](https://pkg.go.dev/github.com/clipperhouse/rate)

I am designing a new, composable rate limiter for Go, with an emphasis on clean API and low overhead. Early days!

Rate limiters are typically an expression of several layers of policy. You might limit by user, by product, or by URL, or all of the above. You might allow short spikes; you might apply dynamic limits; you may want to stack several limiters on top of one another.

This library intends to make all the above use cases expressible, readable and easy to reason about. 

## Installation

```bash
go get github.com/clipperhouse/rate
```

## Example

```go
// Define a getter for the rate limiter “bucket”
func byIP(req *http.Request) string {
    // You can put arbitrary logic in here. In this case, we’ll just use IP address.
    return req.RemoteAddr
}

// 10 requests per second
limit := rate.NewLimit(10, time.Second)

// 10 requests per second per IP
limiter := rate.NewLimiter(byIP, limit)

// In your HTTP handler, where r is the http.Request
if limiter.Allow(r) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("Success"))
} else {
    w.WriteHeader(http.StatusTooManyRequests)
    w.Write([]byte("Too many requests"))
}
```

## Concepts

### `bucket`

The rate-limiting algorithm is a “token bucket”. The bucket begins with _n_ tokens
as defined by your `Limit`.

When you `Allow` a request, a token is deducted from the bucket <sup>[*]</sup>. Requests
are allowed as long as there is at least one token remaining in the bucket.

The bucket is refilled by the passage of time. If you define a limit of 10 req/s,
a new token will be added every 100ms.

<sup>_[*] More precisely, a token is deducted when the request is allowed; if the request
is denied for lack of tokens, no token is deducted, i.e. debt is not incurred._</sup>

### `Keyer`

You define your buckets with a `func` that takes one parameter,
and returns a bucket’s unique identifier (key).

```go
type Keyer[TInput any, TKey comparable] func(input TInput) TKey
```

Go’s type inference makes this read cleanly, don’t worry. To limit by IP address,
for example:

```go
func byIP(req *http.Request) string {
    return req.RemoteAddr
}
```

The resulting limiter will be typed by the input parameter of your `keyer`.
Your `limiter.Allow()` call will take the input type of your `keyer`.

```go
// Somewhere in your HTTP handler, where r is the incoming http.Request:

limiter.Allow(r)
```

I think that’s elegant.

Nothing about this is HTTP-specific, you can use it for anything you wish to rate-limit:

```go
var db = getDBConnection()  // a closure might be handy
func byUserID(email string) int {
    return db.GetUserIDByEmail(email)
}
```

### `Limit`

A `limit` is a count over a period of time, which is tracked in a `bucket`. It is
defined by calling (e.g.) `rate.NewLimit(10, time.Second)`.

```go
perSecond := rate.NewLimit(10, time.Second)
limiter := rate.NewLimiter(byUser, perSecond)
```

You can use arbitrary `time.Duration`’s.

## Tips & tricks

If your `Keyer` requires more than one input, consider a closure:

```go
var db = getDBConnection()
func byUserID(email string) int {
    return db.GetUserIDByEmail(email)
}
```
...or create a new struct type to represent the multiple inputs. If it requires zero inputs,
just use `_`.

If you want different limits for different requests, use `NewLimiterFunc`.

```go
// reads are cheap
readLimit := rate.NewLimit(50, time.Second)
// writes are expensive
writeLimit := rate.NewLimit(10, time.Second)

limitFunc := func(r *http.Request) Limit {
    if r.Method == "GET" {
        return readLimit
    }
    return writeLimit
}
limiter := rate.NewLimiterFunc(keyer, limitFunc)
```

If you want to allow short spikes but prevent sustained ones, create two `rate.Limiter`’s
and call `Allow` in succession.

```go
perSecond := rate.NewLimit(10, time.Second)
perMinute := rate.NewLimit(100, time.Minute)

limiters := []rate.Limiter {
    rate.NewLimiter(keyer, perSecond),
    rate.NewLimiter(keyer, perMinute),
}

for _, limiter := range limiters {
    if limiter.Allow() {
        ...
    }
}
```

## Prior art

The Go team offers [golang.org/x/time/rate](https://golang.org/x/time/rate). What they call
a `limiter` is equivalent to our `bucket` type above.

This package builds on top of that primitive concept, to look up buckets by key, and to
accommodate dynamic limits.
