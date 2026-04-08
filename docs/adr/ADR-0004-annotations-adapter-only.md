# ADR-0004: Annotation Support Is Adapter-Only

## Status
Accepted

## Context
Annotation-driven APIs are useful in Spring but not portable to non-annotation runtimes and can hide critical policy defaults.

## Decision
Core registration API remains programmatic. Optional annotations (for Spring) are syntactic sugar in adapter modules and map to explicit core definitions.

## Consequences
- Shared semantics across frameworks.
- Annotation convenience without coupling core contracts to framework metadata.
