[![Tests](https://github.com/clipperhouse/rate/actions/workflows/tests.yml/badge.svg)](https://github.com/clipperhouse/rate/actions/workflows/tests.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/clipperhouse/rate.svg)](https://pkg.go.dev/github.com/clipperhouse/rate)

A fast and composable rate limiter for Go.

Rate limiters are typically an expression of several layers of policy. You might
limit by user, or by resource, or both. You might allow short spikes; you might
apply dynamic limits; you may want to stack several limits on top of one another.

This library intends to make the above use cases expressible, readable and easy
to reason about.

## Quick start

```bash
go get github.com/clipperhouse/rate
```

```go
// Define a “KeyFunc”, which defines the bucket. It’s generic, doesn't have to be HTTP.
func byIP(req *http.Request) string {
    // You can put arbitrary logic in here. In this case, we’ll just use IP address.
    return req.RemoteAddr
}

limit := rate.NewLimit(10, time.Second)
limiter := rate.NewLimiter(byIP, limit)

// In your HTTP handler:
if limiter.Allow(r) {
    w.WriteHeader(http.StatusOK)
} else {
    w.WriteHeader(http.StatusTooManyRequests)
}
```

## Composability

I intend this package to offer a set of basics for rate limiting, that you can
compose into arbitrary logic, while being easy to reason about. Let's make easy
things easy and hard things possible.

#### One or many limits

You might wish to allow short spikes while preventing sustained load. So a
`Limiter` can accept any number of `Limit`'s:

```go
func byIP(req *http.Request) string {
    // You can put arbitrary logic in here. In this case, we’ll just use IP address.
    return req.RemoteAddr
}

perSecond := rate.NewLimit(10, time.Second)
perMinute := rate.NewLimit(100, time.Minute)

limiter := rate.NewLimiter(byIP, perSecond, perMinute)
```

The `limiter.Allow()` call checks both limits; all must allow or the request is
denied. If denied, it will deduct no tokens from any limit.

#### One or many limiters

```go
func byUser(req *http.Request) string {
    return getTheUserID(req)
}

userLimit := rate.NewLimit(100, time.Minute)
userLimiter := rate.NewLimiter(byUser, userLimit)

func byResource(req *http.Request) string {
    return req.Path
}

resourceLimit := rate.NewLimit(5, time.Second)
resourceLimiter := rate.NewLimiter(byResource, resourceLimit)

combined := rate.Combine(userLimiter, resourceLimiter)

// in your app, a single transactional allow call:

if combined.Allow(r)...

```

#### Dynamic limits

Dynamic == funcs.

```go
// Dynamic based on customer

func byCustomerID(customerID int) int {
    return customerID
}

func getCustomerLimit(customerID int) Limit {
    plan := lookupCustomerPlan(customerID)
    return plan.Limit
}

limiter := rate.NewLimiterFunc(byCustomerID, getCustomerLimit)

// somewhere in the app:

customerID := getTheCustomerID()

if limiter.Allow(customerID) {
    ...do the thing
}
```

```go
// Dynamic based on expense

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
limiter := rate.NewLimiterFunc(keyFunc, limitFunc)
```

#### Dynamic costs

```go

// think of 100 as "a dollar"
limit := rate.NewLimit(100, time.Second)
limiter := rate.NewLimiter(keyFunc, limit)

// decide how many "cents" a given request costs
tokens := decideThePriceOfThisRequest()

if limiter.AllowN(customerID, tokens) {
    ...do the thing
}
```

## Implementation details

We define “do the right thing” as “minimize surprise”. Whether we’ve achieved
that is what I would like to hear from you.

#### Concurrent

Of course we need to handle concurrency. After all, a rate limiter is only
important in contended circumstances.

#### Transactional

For a soft definition of “transactional”. Tokens are only deducted when all
limits pass, otherwise no tokens are deducted. I think this is the right
semantics, but perhaps more importantly, it mitigates noisy-neighbor DOS
attempts.

There is only one call to `Now()`, and all subsequent logic uses that time.
Inspired by databases, where a transaction has a consistent snapshot
view that applies throughout.

#### Efficient

See the [benchmarks package](https://github.com/clipperhouse/rate/tree/main/benchmarks)
for a comparison of various fine Go rate limiters. I think we’re fast.

An `Allow()` call takes around 50ns on my machine. You should usually see zero
allocations.

At scale, one might create millions of buckets, so we’ve minimized the [data
size of that struct](https://github.com/clipperhouse/rate/blob/main/bucket.go).

I had the insight that the state of a bucket is completely expressed by a
timestamp and a `Limit`. There is no `token` type or field; calculating the
available tokens is just arithmetic on time.

You will find `GC()` and `Clear()` methods, which you can call to prevent
unbounded memory growth.

#### Observable

You'll find `Peek`, and `*WithDetails` and `*WithDebug` methods, which give
you the information you'll need to return "retry after" or “remaining tokens”
headers, or do detailed logging.

#### Generic

The `Limiter` type is generic. You'll define the type via the `KeyFunc` that
you pass to `NewLimiter`. HTTP is the common case, but you can use whatever
your app needs.

## Roadmap

You tell me. Open an issue, or [ping me on 𝕏](https://x.com/clipperhouse).

I can imagine a desire to share state across a cluster, perhaps via Redis or
other shared database, [here are some thoughts](https://clipperhouse.com/yagni-distributed-rate-limiter/).

## Benchmarks

```
goos: darwin
goarch: arm64
pkg: github.com/clipperhouse/rate/benchmarks
cpu: Apple M2
BenchmarkClipperhouse/serial-8         	28714730	        41.83 ns/op	       0 B/op	       0 allocs/op
BenchmarkClipperhouse/parallel-8       	91543654	        13.09 ns/op	       0 B/op	       0 allocs/op
BenchmarkSethVargo/serial-8            	22482856	        53.11 ns/op	       0 B/op	       0 allocs/op
BenchmarkSethVargo/parallel-8          	10076662	       118.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkThrottled/serial-8            	13771245	        86.99 ns/op	       0 B/op	       0 allocs/op
BenchmarkThrottled/parallel-8          	 7096096	       168.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkTollbooth/serial-8            	 6205686	       192.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkTollbooth/parallel-8          	 3554658	       336.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkUber/serial-8                 	32043044	        37.27 ns/op	       0 B/op	       0 allocs/op
BenchmarkUber/parallel-8               	16449237	        76.67 ns/op	       0 B/op	       0 allocs/op
BenchmarkUlule/serial-8                	 8705726	       137.5 ns/op	       8 B/op	       1 allocs/op
BenchmarkUlule/parallel-8              	 8045072	       166.4 ns/op	       8 B/op	       1 allocs/op
```

- [sethvargo/go-limiter](https://github.com/sethvargo/go-limiter)
- [throttled/throttled](https://github.com/throttled/throttled)
- [didip/tollbooth](https://github.com/didip/tollbooth)
- [uber-go/ratelimit](https://github.com/uber-go/ratelimit)
- [ulule/limiter](https://github.com/ulule/limiter)

See the [benchmarks package](https://github.com/clipperhouse/rate/tree/main/benchmarks) for details.
