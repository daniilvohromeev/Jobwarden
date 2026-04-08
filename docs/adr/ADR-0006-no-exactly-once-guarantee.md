# ADR-0006: No Exactly-Once Platform Guarantee

## Status
Accepted

## Context
Exactly-once requires end-to-end transactional coordination with every side-effect system, which is outside JGK control and incompatible with many enterprise integrations.

## Decision
JGK does not promise exactly-once. It provides duplicate defense mechanisms: unique schedule constraints, lease fencing tokens, idempotency hooks, and execution ledgers.

## Consequences
- Clear guarantee boundaries.
- Encourages correct system design around idempotent side effects.
