# Job Governance Kit (JGK)

[![Java 21+](https://img.shields.io/badge/Java-21+-orange)](https://openjdk.org/projects/jdk/21/)
[![Build](https://img.shields.io/badge/build-maven-blue)](https://maven.apache.org/)
[![PostgreSQL First](https://img.shields.io/badge/storage-PostgreSQL-336791)](https://www.postgresql.org/)
[![Semantics](https://img.shields.io/badge/delivery-at--least--once-informational)](#delivery-semantics)

Framework-agnostic scheduling and background job governance for Java 21+ applications.

JGK is a governance layer for jobs in clustered environments.
It standardizes scheduling, claiming, retries, timeouts, cancellation, and operational visibility without forcing you into a specific runtime stack.

## Why JGK

Most teams combine multiple tools to solve job execution in production:
cron trigger, distributed lock, retry utility, custom dashboards, and ad-hoc recovery scripts.
That usually creates inconsistent behavior between services and poor operational control.

JGK focuses on one thing: a unified control plane for jobs across JVM runtimes.

## What JGK is and is not

JGK is:
- A modular governance platform for scheduled and background jobs.
- Runtime-agnostic at the core (`Spring`, `WebFlux`, `Reactor`, `Vert.x` adapters on top).
- PostgreSQL-first with SQL-first claim/transition paths.

JGK is not:
- A web framework.
- A DI container.
- A Temporal/BPMN workflow engine.
- An exactly-once execution platform.

## Delivery semantics

- Default guarantee: `AT_LEAST_ONCE`.
- Duplicate execution is possible after crashes or lease loss.
- Exactly-once is intentionally not guaranteed.
- Idempotency is a first-class policy concern and must be handled by application job logic.

## Architecture at a glance

```text
┌─────────────────────────────────────────────────────────────────┐
│ Runtime Adapters                                                │
│ spring-core | spring-webmvc | spring-webflux | reactor | vertx │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│ jgk-executor-core                                               │
│ scheduler materialization | claim | runner | heartbeat |        │
│ retry requeue | recovery                                        │
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│ jgk-core + jgk-storage-spi                                      │
│ domain model | policies | schedule evaluator | storage contracts│
└───────────────────────────────┬─────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────┐
│ jgk-storage-postgres                                             │
│ Flyway migrations | SQL repositories | lease/claim transitions  │
└─────────────────────────────────────────────────────────────────┘
```

## Modules

| Module | Purpose |
|---|---|
| `jgk-core` | Domain model, policies, handler contracts, schedule evaluator |
| `jgk-storage-spi` | Storage contracts for executions, definitions, cursors, audit |
| `jgk-storage-postgres` | PostgreSQL implementation + Flyway migration |
| `jgk-executor-core` | Runtime loops: scheduler, claim, runner, heartbeat, retry, recovery |
| `jgk-observability-micrometer` | Metrics integration |
| `jgk-observability-otel` | Tracing integration |
| `jgk-spring-core` | Spring Boot auto-configuration and engine lifecycle |
| `jgk-spring-webmvc` | MVC admin endpoints |
| `jgk-spring-webflux` | WebFlux admin endpoints |
| `jgk-reactor` | `Mono` / `Flux` handler adapters |
| `jgk-vertx` | Event-loop-safe and blocking bridges for Vert.x |
| `examples/*` | Runnable integration samples |

## Current implementation status

Implemented now:
- Core job model, strict execution statuses, retry/timeout/idempotency policy contracts.
- Built-in retry strategies:
  - fixed delay
  - exponential backoff
  - max attempts
  - retry window cutoff
  - retryable/non-retryable classification
  - jitter
- Deterministic schedule evaluation with timezone-aware cron support.
- PostgreSQL claim/lease/recovery/requeue/finish transitions.
- Scheduler materialization loop and enqueue path.
- Claim loop, runner loop, heartbeat loop, retry requeue loop, recovery loop.
- Spring auto-configuration for engine loops and lifecycle start/stop.

In progress:
- Full persistent schedule cursor integration in storage adapters.
- Full management service implementation behind admin controllers.
- Broader integration/concurrency/TCK coverage for future storage backends.

## Quick start

### Prerequisites

- Java `21+`
- Maven `3.9+`
- Docker (optional, for Testcontainers-backed integration tests)

### Build and test

```bash
mvn test
```

### Run one module during development

```bash
mvn -pl jgk-core -am test
mvn -pl jgk-executor-core -am test
mvn -pl jgk-storage-postgres -am test
```

## Spring Boot usage (auto-config)

`jgk-spring-core` auto-wires the execution engine and loops when `ExecutionRepository` is present.

```yaml
jgk:
  enabled: true
  auto-start: true
  worker-id: ${HOSTNAME:local-worker}
  schedule-batch-size: 128
  claim-batch-size: 64
  max-running-global: 32
  scheduler-interval: 5s
  heartbeat-interval: 30s
  retry-requeue-interval: 5s
  recovery-interval: 30s
```

## Job registration example

```java
JobDefinition definition = new JobDefinition(
    "billing.reconcile",
    1,
    "Reconcile invoices",
    "Nightly billing reconciliation",
    "billing-platform",
    Set.of("billing", "nightly"),
    ExecutionMode.BLOCKING,
    new JobSchedule.CronSchedule("0 5 * * *", ZoneId.of("UTC"), null, null),
    new JobPolicy(
        RetryStrategies.exponentialBackoff(
            new RetryStrategies.ExponentialBackoffConfig(5, Duration.ofSeconds(5), Duration.ofMinutes(5), 2.0)
        ),
        new TimeoutPolicy.DefaultTimeoutPolicy(
            Duration.ofMinutes(15),
            Duration.ofMinutes(5),
            Duration.ofMinutes(10),
            Duration.ofSeconds(30),
            Duration.ofMinutes(2)
        ),
        MisfirePolicy.CATCH_UP_LATEST_ONLY,
        new ConcurrencyPolicy.ForbidOverlap(),
        yourIdempotencyStrategy,
        true
    ),
    "payload-v1",
    JobDefinitionState.ENABLED,
    true,
    false,
    null
);
```

## Examples

- `examples/spring-mvc-postgres`
- `examples/spring-webflux-reactor`
- `examples/vertx-postgres`

## Design docs

- Architecture proposal: `docs/architecture/architecture-proposal.md`
- ADRs: `docs/adr`
- PostgreSQL DDL: `jgk-storage-postgres/src/main/resources/db/migration/V1__init.sql`
- Query playbook: `docs/sql/postgres-query-playbook.sql`

## Roadmap

- Persistent schedule cursor repositories and startup reconciliation.
- Fully functional management service (trigger/pause/resume/cancel/query/audit).
- Broader adapter integration tests (`Spring MVC`, `WebFlux`, `Vert.x`).
- Storage TCK for future backends (`MySQL` and beyond).
- Hardened metrics/tracing surfaces and operational health endpoints.

## Contributing

Contributions are welcome. For architecture decisions, read ADRs first, then open a focused PR with tests.

Recommended local workflow:
```bash
mvn test
```
