[![Tests](https://github.com/clipperhouse/rate/actions/workflows/tests.yml/badge.svg)](https://github.com/clipperhouse/rate/actions/workflows/tests.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/clipperhouse/rate.svg)](https://pkg.go.dev/github.com/clipperhouse/rate)

A new, composable rate limiter for Go, with an emphasis on clean API and low overhead.

Rate limiters are typically an expression of several layers of policy. You might limit by user, or by resource, or both. You might allow short spikes; you might apply dynamic limits; you may want to stack several limits on top of one another.

This library intends to make the above use cases expressible, readable and easy to reason about.

Early days! I want your feedback, on GitHub or [on 𝕏](https://x.com/clipperhouse).

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

I intend this package to offer a set of basics for rate limiting, that you can compose into
arbitrary logic, while being easy to reason about. In my experience, rate limiting gets complicated
in production -- layered policies, dynamic policies, etc.

So, let’s make easy things easy and hard things possible.

#### One or many limits

You might wish to allow short spikes while preventing sustained load. So a `Limiter`
can accept any number of `Limit`’s:

```go
func byIP(req *http.Request) string {
    // You can put arbitrary logic in here. In this case, we’ll just use IP address.
    return req.RemoteAddr
}

perSecond := rate.NewLimit(10, time.Second)
perMinute := rate.NewLimit(100, time.Minute)

limiter := rate.NewLimiter(byIP, perSecond, perMinute)
```

The `limiter.Allow()` call checks both limits; all must allow or the request is denied.
If denied, it will deduct no tokens from any limit.

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

Of course we need to handle concurrency. After all, a rate limiter is
only important in contended circumstances. We’ve worked to make this correct
and performant.

#### Transactional

For a soft definition of “transactional”. Tokens are only deducted when all
limits pass, otherwise no tokens are deducted. I think this is the right semantics,
but perhaps more importantly, it mitigates noisy-neighbor DOS attempts.

There is only one call to `time.Now()`, and all subsequent logic uses that time.
Inspired by databases, where a transaction has a consistent snapshot view that
applies throughout.

#### Efficient

See the `benchmarks/` folder.

You should usually see zero allocations. An `Allow()` call takes
around 50ns on my machine. Here are some
[benchmarks of other Go rate limiters](https://github.com/sethvargo/go-limiter#speed-and-performance).

At scale, one might create millions of buckets, so we’ve minimized the [data
size of that struct](https://github.com/clipperhouse/rate/blob/main/bucket.go).

I had the insight that the state of a bucket is completely expressed by a `time` field
(in combination with a `Limit`). There is no `token` type or field.
Calculating the available tokens is just arithmetic on time.

#### Inspectable

You’ll find `Peek`, and `*WithDetails` and `*WithDebug` methods, which give you the
information you’ll need to return “retry after” or “remaining tokens” headers, or do
detailed logging.

#### Generic

The `Limiter` type is generic. You'll define the type via the `KeyFunc` that you pass to `NewLimiter`.
HTTP is the common case, but you can use whatever your app needs.

## Roadmap

First and foremost, I want some feedback. Try it, open an issue, or [ping me](https://x.com/clipperhouse).
