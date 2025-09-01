(Written for an agent)

This is a plan and design document for creating a new wait method which ensures
first-in-first-out (FIFO) ordering of requests, per key.

The existing, non-FIFO implementation is `Limiter.waitNWithDetails`, in
`limiter_wait.go`. The new implementation will be in `Limiter.waitNWithDetailsFIFO`.
Modify that latter, new method.

Start by learning about the problem. Read this blog post:
https://destel.dev/blog/preserving-order-in-concurrent-go

Then review this package: https://github.com/destel/rill, which
has an implementation that we might be interested in.

One challenge is that there is no event which signals the availability of a token.
Tokens are "created" only by the passage of time. See `bucket.go`.

We probably need a `time.After` to poll for the availability of a token, based on
retryAfter. But that's just a suggestion, if you can do better, that's great.

So perhaps we will end up with a select on 3 channels:
- Request queue
- ctx.Done
- time.After

It might be good to block/select on the request channel enqueue instead of dequeue,
not sure.

After understanding the above, plan how to update `Limiter.waitNWithDetailsFIFO`
to implement FIFO semantics.

Create a simple, preliminary test which will demonstrate basic FIFO. Don't worry
about edge cases yet, start with the basics, and we will iterate. Let's focus on
on clarity and testability first. Once you have a candidate implementation, run
the test and iterate.

Look at other tests in `limiter_test.go` for examples.

If you are unsure about goals or semantics, ask me.
