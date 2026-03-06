# ADR 0003: Consistency and Error Semantics

- Status: Accepted
- Date: 2026-03-06
- Decision Window: Latest implementation phase

## Context
Replay and concurrency guarantees are only useful if violations are detected consistently and reported with stable, typed errors.

## Decision
AtomicFlow adopts strict consistency semantics:

1. **Workflow input consistency**: same workflow instance ID cannot be reused with different input.
2. **Step input consistency**: same step idempotency ID cannot be associated with conflicting step version/input fingerprints.
3. **Signal consistency**: existing signal values are immutable (conflict on change).
4. **Execution exclusivity**: locked workflow instances reject concurrent recovery/run.

Failures are represented using typed exceptions with metadata-rich references:
- `WorkflowNotFoundException`
- `WorkflowLockedException`
- `WorkflowInputConflictException`
- `StepInputConflictException`
- `SignalEmptyException`
- `SignalConflictException`

## Why This Concept Matters
- Prevents silent state drift during replay.
- Gives callers explicit failure classes for retry/fail-fast decisions.
- Keeps behavior aligned across runtime implementations.

## Changes in This Phase
- Enforced conflict checks in both step cache read and write paths.
- Unified exception message generation around metadata constructors.
- Updated runtime call sites to use explicit workflow/step/signal metadata exceptions.

## Consequences
### Positive
- Deterministic and debuggable failure surface.
- Lower risk of hidden corruption in idempotent/cached execution.

### Trade-off
- Previously tolerated inconsistent records now fail fast.

## Related Implementation Files
- `core/src/main/scala/atomicflow/WorkflowException.scala`
- `core/src/main/scala/atomicflow/StepException.scala`
- `core/src/main/scala/atomicflow/SignalException.scala`
- `core/src/main/scala/atomicflow/impl/memory/InMemoryWorkflowRuntime.scala`
- `db/src/main/scala/atomicflow/impl/db/DbWorkflowRuntime.scala`
