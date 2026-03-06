# ADR 0002: Idempotency and Replay Model

- Status: Accepted
- Date: 2026-03-06
- Decision Window: Latest implementation phase

## Context
AtomicFlow is centered on safe replay: rerunning a workflow should not duplicate side effects when inputs and semantics match prior execution.

## Decision
AtomicFlow defines replay through **step idempotency identity + input fingerprints**:

- **Cached steps**: idempotency identity includes step version and input fingerprints.
- **Only-once steps**: idempotency identity is stable per step, independent of input fingerprints.
- **Step cache**: stores output bound to idempotency identity and step metadata.

Replay behavior:
- If a cached value exists for the active identity, step body execution is short-circuited and cached value is used.
- If no cached value exists, step body runs and output is persisted with TTL metadata.

## Why This Concept Matters
- Distinguishes functional replay (`cache`) from side-effect replay suppression (`onlyOnce`).
- Makes replay deterministic and auditable.
- Keeps replay semantics explicit in the step DSL.

## Changes in This Phase
- Consolidated idempotency and cache access around scope-bound store interfaces.
- Standardized input fingerprint usage across replay paths.
- Preserved only-once override behavior while aligning it with scoped execution.

## Consequences
### Positive
- Predictable replay outcomes for both pure and side-effecting steps.
- Clear separation between identity acquisition and value caching.

### Trade-off
- Requires careful step versioning discipline; changing semantics without version changes can break assumptions.

## Deferred Work
- TTL metadata is persisted but strict expiry enforcement is not yet uniformly applied on read paths.

## Related Implementation Files
- `core/src/main/scala/atomicflow/Step.scala`
- `core/src/main/scala/atomicflow/internal/StepIdempotencyStore.scala`
- `core/src/main/scala/atomicflow/internal/StepCache.scala`
- `core/src/main/scala/atomicflow/internal/StepInputFingerprints.scala`
- `core/src/main/scala/atomicflow/impl/memory/InMemoryWorkflowRuntime.scala`
- `db/src/main/scala/atomicflow/impl/db/DbWorkflowRuntime.scala`
- `db/src/main/resources/db/migration/V001__init.sql`
- `db/src/main/resources/db/migration/V002__overridden.sql`
