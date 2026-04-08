# Job Governance Kit (JGK)

Job Governance Kit is a framework-agnostic scheduling and background job governance platform for Java 21+.

## Modules

- `jgk-core`: runtime-agnostic domain model and public APIs.
- `jgk-storage-spi`: persistence contracts used by executor and adapters.
- `jgk-storage-postgres`: PostgreSQL-first SQL implementation and migrations.
- `jgk-executor-core`: scheduler, claim, runner, heartbeat, recovery, cleanup loops.
- `jgk-observability-micrometer`: metrics bridge.
- `jgk-observability-otel`: tracing bridge.
- `jgk-spring-core`: Spring Boot starter and auto-configuration.
- `jgk-spring-webmvc`: management API for MVC apps.
- `jgk-spring-webflux`: management API for WebFlux apps.
- `jgk-reactor`: Reactor handler adapters and context propagation.
- `jgk-vertx`: Vert.x handler adapters and event-loop safe execution routing.
- `examples/*`: sample applications.

## Design Docs

- Architecture proposal: `docs/architecture/architecture-proposal.md`
- ADRs: `docs/adr`
- PostgreSQL DDL: `jgk-storage-postgres/src/main/resources/db/migration/V1__init.sql`
- Query playbook: `docs/sql/postgres-query-playbook.sql`
