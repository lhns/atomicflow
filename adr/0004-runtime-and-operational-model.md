# ADR 0004: Runtime and Operational Model

- Status: Accepted
- Date: 2026-03-06
- Decision Window: Latest implementation phase

## Context
AtomicFlow targets multiple runtime backends and must remain operable for contributors across local and CI environments.

## Decision
AtomicFlow runtime/operations model is:

1. **Runtime portability**: one public runtime contract with interchangeable backends (in-memory and DB).
2. **Schema-backed durability (DB runtime)**: workflow, idempotency, cache, and signal state are persisted in migrations.
3. **Operationally safe tests**: DB test suite is opt-in when required environment is configured.
4. **Local tooling hygiene**: editor/build metadata is excluded from version control.

## Why This Concept Matters
- Keeps core behavior stable while enabling backend-specific trade-offs.
- Makes contributor experience predictable without forcing local DB setup.
- Reduces repository noise and accidental commits.

## Changes in This Phase
- Completed migration of DB runtime internals to the scope-bound model.
- Preserved DB schema support for signals and idempotency override semantics.
- Updated DB suite to skip when `DB_URL`, `DB_USERNAME`, or `DB_PASSWORD` is missing.
- Added README test prerequisites and expanded `.gitignore` for local tooling files.

## Consequences
### Positive
- Practical local development flow and stable CI behavior.
- Clear separation of conceptual runtime guarantees vs operational prerequisites.

### Trade-off
- DB test coverage can be absent unless environment is intentionally provided.

## Deferred Work
- TTL enforcement semantics should be aligned across in-memory and DB read paths.
- Runtime lifecycle ownership (DB resources) should be explicitly documented for embedding applications.

## Related Implementation Files
- `core/src/main/scala/atomicflow/WorkflowRuntime.scala`
- `core/src/main/scala/atomicflow/impl/memory/InMemoryWorkflowRuntime.scala`
- `db/src/main/scala/atomicflow/impl/db/DbWorkflowRuntime.scala`
- `db/src/main/resources/db/migration/V001__init.sql`
- `db/src/main/resources/db/migration/V003__signals.sql`
- `db/src/test/scala/test/DbWorkflowRuntimeSuite.scala`
- `README.md`
- `.gitignore`
