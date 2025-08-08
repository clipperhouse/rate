# Go Rate Limiter Library - Coding Instructions

## Design goals

### Composability
- This library is intended to allow users to create complex rate limiting policies by composing elements
- Users should be able to mix and match different limiters, keyers, and strategies to create the desired behavior.
- The library should behave predictably given the user's configuration. It should be easy to reason about by users.
- We wish to be like Go's standard library: simple, composable, and predictable.
- The library will abstract away the difficult or tedious parts of rate limiting, such as concurrency and timing issues, so users can focus on their application logic. Make easy things easy, and hard things possible.

## Testing Conventions

When I ask you write tests, I am looking for bugs in my implementation. Try to discover bugs and edge cases.

### Race Detection
- Always run Go tests with the `-race` flag to catch race conditions and ensure clean concurrency
- Essential for this codebase since it heavily uses concurrent operations
- Use 10 second timeout for tests to prevent hanging
- The agent should run tests directly if possible, instead of asking me to do it

### Test Structure Patterns
- Every test function must start with `t.Parallel()` to enable concurrent test execution
- Use comprehensive test naming: `TestType_Method_Scenario` (e.g., `TestLimiter_Allow_MultipleBuckets_Concurrent`)
- Create test variations covering: single/multiple buckets, single/multiple limits, serial/concurrent access.
- The t.Run subtest style is good if the tests are seem combinatorial, same for benchmarks and b.Run
- When you assert that something has been fixed or improved, run tests.
- Do not ask me to run tests or benchmarks, just run them yourself.

### Concurrent Testing Requirements
- Always test both serial and concurrent versions of functionality
- Use `sync.WaitGroup` for coordinating concurrent operations
- Test scenarios with oversubscription (more goroutines than available tokens)
- Include race condition tests with high concurrency (e.g., 100+ goroutines)

### Testing Helpers & Patterns
- Use `require.True/False` with descriptive error messages including context variables
- Format error messages with variable interpolation: `"bucket %d should allow request", bucketID`
- Test both success and failure paths for all rate limiting operations
- Always verify state after operations (remaining tokens, bucket exhaustion, etc.)

## Code Structure

### Comments
- Don't add comments for fairly obvious things, if they seem repetitive with the code itself

### Generic Types
- Maintain generic type constraints: `[TInput any, TKey comparable]`
- Use clear, descriptive type parameter names
- Implement both static limits (`NewLimiter`) and dynamic limits (`NewLimiterFunc`)

### Token Bucket Algorithm
- Follow token bucket refill patterns based on time passage
- Calculate `durationPerToken` correctly for refill timing
- Test token exhaustion and refill behavior thoroughly
- Always test boundary conditions (exactly at limit, just over limit)

### Concurrency Safety
- Use `sync.Mutex` or similar primitives for shared state
- Implement concurrent-safe data structures (like `syncMap`)
- Test with high concurrency to surface race conditions
- Verify thread-safety with tools like `go test -race`

## API Design Patterns
- Provide both simple (`Allow`, `Peek`) and detailed (`AllowWithDetails`, `PeekWithDetails`) API variants
- Use time-based parameters for deterministic testing
- Return structured details including remaining tokens, execution time, and bucket keys
- Implement `Wait` functionality with proper context handling and cancellation
