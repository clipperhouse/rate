# Go Rate Limiter Library - Coding Instructions

## Testing Conventions

### Race Detection
- Always run Go tests with the `-race` flag to catch race conditions and ensure clean concurrency
- Essential for this codebase since it heavily uses concurrent operations

### Test Structure Patterns
- Every test function must start with `t.Parallel()` to enable concurrent test execution
- Use comprehensive test naming: `TestType_Method_Scenario` (e.g., `TestLimiter_Allow_MultipleBuckets_Concurrent`)
- Create test variations covering: single/multiple buckets, single/multiple limits, serial/concurrent access

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
