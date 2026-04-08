# ADR-0007: JGK Is Not a Workflow Engine

## Status
Accepted

## Context
Workflow engines solve orchestration, durable steps, and saga state machines. This is a different product scope with larger runtime complexity.

## Decision
JGK focuses on job governance primitives: scheduling, claiming, retries, leases, cancellation, metadata, observability, and control APIs.

## Consequences
- Smaller and clearer product scope.
- Better fit as shared governance infrastructure in existing application architectures.
