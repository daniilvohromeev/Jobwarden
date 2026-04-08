# ADR-0001: Framework-Agnostic Core

## Status
Accepted

## Context
Target users run Spring MVC, WebFlux, Reactor-only, and Vert.x services. A framework-coupled core would block adoption and create hidden runtime assumptions.

## Decision
`jgk-core` contains only Java 21 domain contracts and policy logic with no Spring/Vert.x/Reactor APIs. Framework integration is adapter-only.

## Consequences
- Portable core and consistent behavior across runtimes.
- Adapter modules own lifecycle/context/threading integration.
- Slightly more upfront adapter work, but cleaner long-term compatibility.
