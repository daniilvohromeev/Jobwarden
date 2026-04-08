# Job Governance Kit Architecture Proposal

## 1. Context and Goals

Job Governance Kit (JGK) standardizes scheduling and background job governance across Java runtimes without requiring framework lock-in. It targets:

- Java 21+ services in Kubernetes/multi-instance deployments.
- Mixed blocking and non-blocking workloads.
- At-least-once, lease-based execution with explicit idempotency contracts.
- PostgreSQL-first operation, with SQL-backend extensibility through SPI.

## 2. Non-Goals

- No workflow/BPM engine or saga DSL.
- No custom consensus or cluster membership protocol.
- No exactly-once claim.
- No Spring-only abstraction.

## 3. Recommended Delivery Semantics

- Default: **at-least-once**.
- Duplicates can occur after crash or lease expiry.
- Idempotency strategy is mandatory in policy model.
- Recurring jobs default to no overlap for same schedule key.

## 4. High-Level Architecture

```text
                    +-------------------------------+
                    |      Runtime Adapters         |
                    | Spring / WebFlux / Vert.x     |
                    +---------------+---------------+
                                    |
                                    v
+----------------------+   +--------+---------+   +------------------------+
|  Observability       |   |  Executor Core   |   |  Core Domain & APIs    |
|  Micrometer / OTel   +-->+ loops + policies +<->+ schedules + state      |
+----------------------+   +--------+---------+   +------------------------+
                                    |
                                    v
                         +----------+-----------+
                         |   Storage SPI        |
                         +----------+-----------+
                                    |
                                    v
                         +----------+-----------+
                         | PostgreSQL Adapter   |
                         | SQL + Flyway         |
                         +----------------------+
```

## 5. Module Decomposition

- `jgk-core`: job model, policies, handler contracts, schedule evaluator contract.
- `jgk-storage-spi`: execution/definition/audit/trigger persistence contracts.
- `jgk-storage-postgres`: migration + SQL-first repository implementations.
- `jgk-executor-core`: scheduler loop, claim loop, runner loop, heartbeat/recovery/cleanup loops.
- `jgk-observability-*`: metrics and tracing adapters.
- `jgk-spring-*`, `jgk-reactor`, `jgk-vertx`: integration layers.

## 6. Domain Model

- Logical definition: `JobDefinition` (key, version, owner, tags, mode, schedule, policy, schema version, state).
- Runtime unit: `JobExecution` (execution id, trigger type, timestamps, lease/fencing metadata, result/error summaries).
- Schedules: one-time, fixed delay, fixed rate, cron, disabled.
- Policies: retry, timeout, misfire, concurrency, idempotency.

## 7. State Machine

```text
REGISTERED -> SCHEDULED -> CLAIMED -> RUNNING -> SUCCEEDED
                                  \-> FAILED_RETRYABLE -> SCHEDULED
                                  \-> FAILED_FINAL
                                  \-> TIMED_OUT
SCHEDULED -> MISFIRED | SKIPPED
CLAIMED/RUNNING -> CANCEL_REQUESTED -> CANCELLED
CLAIMED/RUNNING (lease expired) -> SCHEDULED or DEAD
```

Definition state is separate: `REGISTERED | ENABLED | PAUSED | DISABLED`.

## 8. Scheduling Design

- Hybrid cursor-based materialization.
- Scheduler loop evaluates due windows by DB time, writes `SCHEDULED` executions.
- Cursor stored in `job_schedule` (`next_materialize_at`, `last_evaluated_at`, `cursor_version`).
- Misfire policy resolved during materialization, not in runner.

## 9. Execution Design

- Worker claim loop fetches due ids and CAS-claims rows.
- Runner starts only claimed rows within local capacity (`maxClaimedNotStarted`, `maxRunningGlobal`, per-job-key limits).
- Heartbeat loop renews leases for long running executions.
- Recovery loop requeues or marks dead after lease expiry.
- Cleanup loop applies retention and dead-letter compaction.

## 10. Persistence Strategy

- PostgreSQL first, SQL-driven claim/transition paths.
- Every transition uses status + lease token + worker identity checks.
- Fencing token increments on each claim to protect side-effect hooks.
- Partial indexes for due claim scans and active lease scans.
- Idempotency uniqueness via partial unique index and business key fields.

## 11. Time Source Strategy

- Use database clock (`CURRENT_TIMESTAMP`) for due/lease transitions.
- Application clock used only for local timers and request timestamps.
- Persist both when needed for diagnostics; DB clock is authoritative.

## 12. Observability Strategy

- Structured logs include job key, execution id, attempt, worker id, trigger type, trace/correlation ids.
- Micrometer counters/timers for scheduling, claim, run, retry, timeout, cancel, contention, queue lag, lateness.
- OpenTelemetry spans:
  - `jgk.schedule.evaluate`
  - `jgk.execution.claim`
  - `jgk.execution.run`

## 13. Security and Multi-Tenancy

- Manual control APIs require explicit adapter-level authn/authz.
- Internal/admin-only jobs flagged in definition metadata.
- Payload references stored separately; logs avoid payload content by default.
- Tenant discriminator supported in definitions/executions/trigger requests and filtered query APIs.

## 14. Failure Semantics

Designed for:

- Crash before/after claim.
- Crash after side effect but before persistence.
- DB outage and restart.
- Lease loss after long GC pauses.
- Duplicate manual trigger requests.
- Stuck claims and stale worker recovery.

Documented guarantee: at-least-once with cooperative cancellation only.

## 15. Migration and Rollout

1. Deploy schema migration first (`V1__init.sql`).
2. Deploy workers in passive mode (scheduler disabled) to validate visibility APIs.
3. Enable scheduling loop in one canary worker.
4. Scale worker count with claim contention and queue lag SLOs.
5. Enable cleanup policies after retention baselines are validated.

## 16. Zero-Downtime Schema Guidance

- Use expand-migrate-contract strategy.
- Add nullable columns first; deploy writers; backfill; enforce constraints after all readers upgraded.
- Avoid dropping columns in same release as producer changes.
