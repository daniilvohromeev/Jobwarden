# ADR-0003: PostgreSQL as First-Class Storage Backend

## Status
Accepted

## Context
MVP requires robust distributed claim/lease semantics with operationally known behavior and strong SQL tooling.

## Decision
PostgreSQL is the first production backend. SQL-first claim/update paths and migration scripts are delivered before any other database backend.

## Consequences
- Faster path to a production-ready backend.
- Clear baseline for behavior and performance.
- Other SQL engines can be added later via `jgk-storage-spi` and TCK tests.
