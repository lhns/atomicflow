# ADR 0001: Workflow Execution Model

- Status: Accepted
- Date: 2026-03-06
- Decision Window: Latest implementation phase

## Context
AtomicFlow needs a clear execution model that explains how workflows and steps are scoped, when execution is considered valid, and how stateful behavior is attached to a single workflow instance.

## Decision
AtomicFlow adopts a scope-based execution model with two conceptual levels:

1. **Workflow Scope**: identity of a workflow execution (`workflowMeta`, `workflowInstanceId`).
2. **Step Scope**: identity of a step execution (`stepMeta`) within a workflow scope.

Execution behavior is defined by these invariants:
- A workflow instance is created once per `(workflowId, workflowInstanceId, input)` identity.
- Recovery/run executes under workflow-level lock semantics.
- Step-side state access (idempotency/cache) is always performed in step scope.

## Why This Concept Matters
- Keeps the mental model simple: all runtime state reads/writes are scope-relative.
- Avoids hidden global context and accidental cross-workflow leakage.
- Provides a stable foundation for idempotency and replay decisions.

## Changes in This Phase
- Introduced explicit `WorkflowScope` and `StepScope` concepts in internals.
- Rewired workflow and step context plumbing to resolve state through active scope.
- Migrated both in-memory and DB runtimes to this scope-based execution model.

## Consequences
### Positive
- Stronger conceptual boundaries between workflow and step concerns.
- More maintainable runtime internals and clearer debugging context.

### Trade-off
- Requires additional internal types and binding wiring.

## Related Implementation Files
- `core/src/main/scala/atomicflow/internal/ExecutionScope.scala`
- `core/src/main/scala/atomicflow/WorkflowContext.scala`
- `core/src/main/scala/atomicflow/StepContext.scala`
- `core/src/main/scala/atomicflow/Step.scala`
- `core/src/main/scala/atomicflow/WorkflowRuntime.scala`
- `core/src/main/scala/atomicflow/impl/memory/InMemoryWorkflowRuntime.scala`
- `db/src/main/scala/atomicflow/impl/db/DbWorkflowRuntime.scala`
