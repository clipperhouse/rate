Forked from [github.com/sethvargo/go-limiter/](https://github.com/sethvargo/go-limiter/tree/main/benchmarks)

This sub-package of [github.com/clipperhouse/rate](https://github.com/clipperhouse/rate) compares
various Go rate limiters.

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

Notes:

- The benchmarks above are only in-memory, with no GC. @sethvargo's [original benchmarks](https://github.com/sethvargo/go-limiter/tree/main/benchmarks) include more scenarios, such as sweeping (GC) and Redis stores.
- Uber's rate limit is just one "bucket", it's a smaller primitive than other implementations,
which manage multiple buckets. Maybe not apples-to-apples.
