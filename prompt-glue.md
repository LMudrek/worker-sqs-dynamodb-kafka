Analyze the entire AWS Glue Job codebase before making any changes.

## Context

This Glue Job reads a Data Mesh table from the Glue Catalog, processes billions of records, groups them by a unique identifier (the DynamoDB partition key), and writes millions of aggregated items into DynamoDB.

Expected scale:
- Input: billions of records.
- Output: millions of DynamoDB items.
- Each DynamoDB item is the aggregation of all records sharing the same unique identifier.

## Current Problem

The current implementation performs a `collect()` after the aggregation phase to write data into DynamoDB.

This causes the Driver to become a bottleneck, leading to memory exhaustion and Glue Job failures (exit code 137).

Observed behavior:
- G.1X (10 workers)
  - ~70k unique IDs → Success
  - ~150k unique IDs → Failure
- G.2X (10 workers)
  - ~350k unique IDs → Failure

Increasing worker size is not an acceptable solution. The processing architecture must be redesigned.

## Objectives

Refactor the job to be:
- Fully distributed.
- Highly scalable.
- Memory efficient.
- Extremely fast.
- Production-ready.

The solution must:
- Eliminate any Driver bottleneck.
- Never use `collect()`, `toPandas()`, `toLocalIterator()`, or any operation that materializes large datasets on the Driver.
- Keep processing distributed until the DynamoDB write phase.
- Maximize executor parallelism.
- Minimize shuffle and memory usage.
- Scale to millions of unique IDs without increasing Glue worker size.

## Constraints

- The current codebase is intentionally simple and mostly organized as a single Glue Job.
- Avoid introducing unnecessary architectural complexity, excessive abstractions, or over-engineering.
- A `utils/` package is available and may be used to extract reusable logic, improve separation of concerns, and increase maintainability when justified.
- Keep the project structure simple, scalable, and easy to understand.
- Any new modules should have a clear purpose and contribute to readability, testability, or reuse.
- The final solution should remain easy to migrate to Amazon EMR running native Spark.

## Guidelines

- Prefer native Spark APIs.
- Use Glue-specific APIs only when they provide a clear advantage while remaining portable to EMR Spark.
- Preserve the existing business logic.
- Perform DynamoDB writes directly from executors (e.g. `foreachPartition` or a superior distributed approach).
- Review partitioning strategy, shuffle behavior, data skew, serialization, caching, persistence, and executor memory usage.
- Optimize the aggregation strategy if a more efficient implementation exists.
- Use DynamoDB BatchWrite with proper retry and exponential backoff for throttling.
- Follow Spark, Glue, Python, DynamoDB, SOLID, and Clean Code best practices.
- Optimize for throughput, scalability, and operational simplicity rather than adding unnecessary layers of abstraction.

## Code Quality

The final implementation must be:
- Clean.
- Modular.
- Maintainable.
- Well documented.
- Easy to migrate to EMR.
- Focused on throughput and low memory consumption.

## Testing

Update or create unit tests covering all modified behavior.

Tests should validate:
- Aggregation correctness.
- DynamoDB payload generation.
- Failure and retry scenarios.
- Edge cases.
- Existing behavior regression.

All tests must pass after the refactoring.

## Deliverables

1. Root cause analysis.
2. Performance bottleneck analysis.
3. Proposed architecture and rationale.
4. Complete refactored implementation.
5. Explanation of every major design decision.
6. Before vs. after architecture comparison.
7. Performance and scalability improvements.
8. Updated unit tests with full coverage for the refactored components.