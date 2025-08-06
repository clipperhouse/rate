[![Tests](https://github.com/clipperhouse/rate/actions/workflows/tests.yml/badge.svg)](https://github.com/clipperhouse/rate/actions/workflows/tests.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/clipperhouse/rate.svg)](https://pkg.go.dev/github.com/clipperhouse/rate)

I am designing a new, composable rate limiter for Go, with an emphasis on clean API and low overhead.

Rate limiters are typically an expression of several layers of policy. You might limit by user, by product, or by URL, or all of the above. You might allow short spikes; you might apply dynamic limits; you may want to stack several limits on top of one another.

This library intends to make all the above use cases expressible, readable and easy to reason about.

Early days! I want your feedback, here on GitHub or [on 𝕏](https://x.com/clipperhouse).

## Quick start

```bash
go get github.com/clipperhouse/rate
```

```go
// Define a getter for the rate limiter “bucket”
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

#### One or many

My first concept of composability is that you can have one or many of anything, and
the library will do the right thing.

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
And, it will only consume tokens when _all_ limits allow.

#### Dynamic

Rate limiters often need arbitrary logic, depending on your app. The best way to express
this is funcs, instead of some sort of config.

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
limiter := rate.NewLimiterFunc(keyer, limitFunc)
```


```go
// Dynamic based on expense, another way

// think of 100 as "a dollar"
limit := rate.NewLimit(100, time.Second)
limiter := rate.NewLimiter(keyer, limit)

// somewhere in the app:

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

There is only one call to `time.Now()`, and all subsequent logic uses that time
-- instead of calling `time.Now()` again a millisecond later. Inspired by databases,
where a transaction has a consistent snapshot view that applies throughout.

#### Efficient

We’ve worked hard to have minimal (often zero) allocations, minimize branching,
etc. An `Allow()` call takes around 60ns on my machine. Here are some [benchmarks
of other Go rate limiters](https://github.com/sethvargo/go-limiter#speed-and-performance).

At scale, one might create millions of buckets, so we’ve minimized the [data
size of that struct](https://github.com/clipperhouse/rate/blob/main/bucket.go).

I had an insight that the state of a bucket is completely expressed by a `time` field
(in combination with a `Limit`). There is no `token` type or field.
Calculating the available tokens is just arithmetic on time.

#### Inspectable

You’ll find `*WithDetails` and `*WithDebug` methods, which give you the information
you’ll need to return “retry after” or “remaining tokens” headers, or do
detailed logging.

## Roadmap

First and foremost, I want some feedback. Please try it and open an issue, or [ping me](https://x.com/clipperhouse).

I’d like to be able to stack multiple `Limiter`s into a single `Limiter`.
Maybe you want to limit first by customer, then by path, then by region,
then globally. [TODO open an issue]

We may wish to have a shared store (e.g. Redis) to allow multiple machines to share limits,
or have persistence. The current implementation is a map in memory.
