# ADR-0005: Reactor and Vert.x Belong to Adapter Layer

## Status
Accepted

## Context
Reactor and Vert.x have distinct threading and context models. Pulling them into core would leak runtime constraints and block other runtimes.

## Decision
Core supports synchronous and `CompletionStage` contracts only. Reactor and Vert.x bridging lives in `jgk-reactor` and `jgk-vertx`.

## Consequences
- Core stays minimal and runtime-neutral.
- Event-loop safety and scheduler dispatch are handled explicitly per adapter.
