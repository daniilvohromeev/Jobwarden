# ADR-0002: At-Least-Once Delivery as Baseline

## Status
Accepted

## Context
In distributed execution with leases and crashes, eliminating duplicates without transactional side-effect coordination is impractical for general JVM applications.

## Decision
JGK guarantees at-least-once execution. Duplicate execution is possible after crash, lease loss, or recovery races. Idempotency strategy is a first-class contract.

## Consequences
- Honest and implementable guarantee.
- Application logic must treat idempotency as part of job correctness.
- Strongly reduces false confidence and hidden data corruption risk.
