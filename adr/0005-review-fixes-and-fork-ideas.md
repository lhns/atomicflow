# ADR 0005: Review Fixes and Ideas Adopted from the tschuchortdev Fork

- Status: Accepted
- Date: 2026-09-30
- Decision Window: feature/next, after sub-workflows and awaiting steps
- Supersedes: the exception list of ADR 0003 (now the sealed `WorkflowError`), the only-once semantics of ADR 0002,
  and the lock semantics implied by ADR 0004

## Context
A review of `feature/next` found correctness gaps:
- the DB lock was a SELECT followed by an unconditional UPDATE, and was released unconditionally;
- `onlyOnce` was really at-least-once: a crash between the side effect and storing its result repeated the effect;
- business code could swallow the pending state with `Try` / `NonFatal`;
- child runs ignored the parent's cache TTL;
- nothing woke up pending instances.

At the same time, [tschuchortdev/atomicflow](https://github.com/tschuchortdev/atomicflow) (branch `impl`) explored the
same direct-style, replay-based model much further. Its authoring model is close to ours, but most of its complexity
comes from a few design choices we do not want. We adopted the cheap, high-value ideas and kept our simpler model.

## Decision

### Kept
- **Signals are write-once values** (Deferred-like), scoped per instance. A stream of events is modeled as keyed
  write-once signals, e.g. one child instance per revision / attempt, keyed by a business id.
- **Identity by derivation**: child and business-keyed instance ids are name-based UUIDs. External actors compute
  them, so no signal inheritance is needed.
- **Retries are business logic**, keyed by domain identities, not by library counters.

### Fixed
- **Execution lock**: atomic conditional acquisition with an owner token, release only by the owner, renewal at
  checkpoints for the whole ancestor chain, and step writes fenced on lock ownership.
- **Honest at-most-once**: a durable *Started* marker before the body runs. An unknown outcome raises
  `StepUnknownState`; whether an exception means "did not happen" is a flag (`retryIf`, `strict`).
- **Pending cannot be swallowed**: inside bodies it travels as a control throwable. It is unwrapped at the public
  boundary, because munit, `Future` and cats-effect treat control throwables as fatal.
- **Child runs** inherit the cache TTL and know their parent and root. Idempotency overrides are not inherited:
  they are globally unique ids.

### Adopted from the fork (in simpler form)
- **Commit before observation**: the first run observes the value round-tripped through its `Cacheable`.
- **Wakeups + worker**: a wakeup per root instance, and a `WorkflowWorker` that recovers due roots. Evaluation stays
  self-sufficient: the worker only calls `recover()`. The fork's lease/fencing/wakeup design is reduced to a lock
  owner token plus one wakeup row per root.
- **`versionAtCreation`**, and no version on at-most-once steps.
- **Input drift policies**: `invalidateOn` / `ensureUnchanged` per input.
- **`Cacheable.withFallback`** for evolving cached formats.
- **Cancellation that stays requested**, checked at checkpoints, with `uncancellable` regions for compensation.

### Deliberately not adopted
- **Signals as an event stream with per-key cursors** and a global sequence. This costs a global advisory lock on
  every send, cursor and subscription tables, and a filter-skip hazard. Keyed write-once signals cover our use cases.
  If a real queue is ever needed, derive the consumer position from replay (the n-th await reads slot n) instead of
  persisting cursors.
- **Signal inheritance to children**: derived child ids make children addressable.
- **Fair races across signals, timers and completions** (`awaitRace`), **`firstToRunWithoutSuspension`**,
  **Updates**: high complexity, and partly self-described as unfinished in the fork.
- **Caching failures**: this requires an application-global `Cacheable[Throwable]`. We retry failed runs instead.
- **Restartable regions, `continueAsNew`, fork/reset**: only needed for unbounded-history workflows. Revisit then.

## Consequences
### Positive
- The runtime guarantees now match their names: exclusive runs, at-most-once side effects, no silently swallowed
  pending state.
- Long-running workflows run without polling, survive process crashes, and can be cancelled and compensated.
- Workflows can evolve safely: versions, drift policies, format fallbacks.

### Trade-off
- More schema (V004–V009) and a slightly larger runtime SPI (parent context, wakeups, cancellation).
- A single step body must finish within the lock timeout, because the lock is only renewed at checkpoints.
- Cancellation checks and lock renewal add a query per new step and ancestor in the DB runtime.

## Related Implementation Files
- `core/src/main/scala/atomicflow/Step.scala`
- `core/src/main/scala/atomicflow/WorkflowContext.scala`
- `core/src/main/scala/atomicflow/WorkflowError.scala`
- `core/src/main/scala/atomicflow/WorkflowWorker.scala`
- `core/src/main/scala/atomicflow/StepInput.scala`
- `core/src/main/scala/atomicflow/impl/memory/InMemoryWorkflowRuntime.scala`
- `db/src/main/scala/atomicflow/impl/db/DbWorkflowRuntime.scala`
- `db/src/main/resources/db/migration/V004__lock_owner.sql` … `V009__cancel.sql`
