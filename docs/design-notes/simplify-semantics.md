# Simplification, part 2: store each fact once

Stacked on `draft/simplify-runtime`, which preserves the specified behaviour.
This branch changes the specified behaviour deliberately, as offered by the PR
author: race order by timestamp, and fewer wakeup mechanisms.

## Goal

Every fact is stored once. Every wakeup and every cleanup is done by exactly
one mechanism. Out of scope:
- the key-value runtime interface;
- sequential branches;
- removing the global advisory lock (it still orders signals for cursors and
  inheritance).

## Items, in order

### D. `generation` from a sequence
- `generation` is drawn from a new sequence `workflow_generation_seq` when an
  instance is created, reset or continued-as-new, instead of `+1` / default 0.
- Why: a recreated parent (discard-existing) otherwise restarts at 0, so its
  children's derived scopes collide with the old children's.
- Tests that use literal `@0` scopes (`ChildCloseSuite`) read the generation
  instead.
- Add a test: discard-and-recreate a parent that has a child; the new parent's
  child is a new instance, and the old child's completion doesn't resolve the
  new parent's await.

### A. The event log holds signals only
- Delete the event kinds `TimerFired` and `WorkflowCompleted`, the random
  `timer_id`, and the timer sweep's fire path (`fireTimerSubscriptionQ`,
  `fireDueTimerSubscriptionsQ`).
- **Timer.** A timer candidate is a timer subscription whose *stored* deadline
  is `<= now`. Its time is that deadline. The stored deadline always wins over
  the deadline that `Timer(delay)` recomputes on every pass.
- **Completion.**
  - Add `terminal_at TIMESTAMPTZ` to `workflow_instances`, set from the runtime
    clock in every terminal transition.
  - A completion candidate is the target row with `terminal_state IS NOT NULL`.
    Its time is `terminal_at`, and its payload is `terminal_outcome`, the same
    payload the event carried.
  - Keep today's behaviour for a deleted or recreated target. Check what it is
    first and write it down in the spec.
- **Race rule.**
  1. Take the earliest accepted signal by `sequence_id`.
  2. A due timer or completion wins over it if its time is not later than that
     signal's `created_at`.
  3. Several due timers and completions are ordered by time, then by declared
     branch order.
  4. Cursor advancement for signals is unchanged.
- **Clock skew.** `created_at`, `terminal_at` and deadlines come from the app
  clocks. The spec promise "a signal arriving after the deadline loses" holds
  only up to the clock skew between nodes. State that in
  `spec/signals-timers.md`, replacing the eager-firing ("race predictability")
  rationale.
- **Timer sweep (wake-only).** Select non-terminal instances with a timer
  subscription whose deadline is `<= now`, and whose
  `last_run_at IS NULL OR last_run_at < deadline`. Upsert a wakeup for each.
  - `last_run_at` is the "handled" marker: a run that started after the
    deadline has evaluated or orphaned the timer.
  - Locks: the instance row only, then the wakeup.
  - No per-row flag.
- **Terminal transitions** no longer append. They no longer take the advisory
  lock either, unless they still delete or append signal events.
- **Fault hooks.** Drop the hooks whose code path is gone
  (`afterTimerFireCommit`, …). Keep the coverage of every remaining hook.

### B. Retry deadline stored once
- After A, the `__retry__` timer subscription is the only source the sweep
  reads. Drop `workflow_steps.retry_deadline` and read the deadline from the
  subscription.
  - This applies only if decoding the step state needs the column for nothing
    but the wait. If something else needs it, keep the column, drop the
    duplicate in the subscription, and report why.
- Fork's "skip rows with a pending retry" filter changes to match.

### C. Wakeups at the writer
- **Before C:**
  - extend `stuckInstances()` to inherited signals and to completions;
  - add a crash test that crashes between the commit and the after-commit
    wake (FaultInjector hook);
  - add two deterministic two-connection tests: subscription registration
    versus `sendSignal`, and a parent and child each waiting on the other
    (completion await versus inherited-signal await).
- **Direct signals.** `sendSignal` upserts the addressee's wakeup in its own
  transaction, after locking the instance row and before the advisory lock,
  when a matching signal subscription exists.
  - Safe: registration fences the same instance row, so the two serialize.
- **Inherited signals and completions.** Wake them in a separate transaction
  right after the writer's commit. That transaction holds no instance row lock:
  it only inserts wakeups, which take `FOR KEY SHARE`.
  - Never in the writer's transaction: a completing child that wakes its
    parent there would lock descendant before ancestor, which can cycle with
    `deleteWorkflowInstancesByPrefix`.
- **Delete** the watermark dispatcher (`dispatchNewEvents`), the
  `workflow_event_dispatch` table and `eventDispatchBatchSize`.
- **Keep the full subscription scan.** It is the only safety net for a crash
  between commit and after-commit wake, and for the READ COMMITTED gap in
  cross-instance cases.
- Update `spec/running-workflows.md` "Sources of wakeups" and
  `spec/signals-timers.md:558`. The async rationale ("no deadlock against
  subscribers") is now met by the lock order instead.

## Lock order (from `draft/simplify-runtime`, still binding)

1. The instance row (`FOR NO KEY UPDATE`, or the fence `UPDATE`) comes first.
2. Then that instance's own rows and wakeup.
3. The advisory append lock comes last.
4. A transaction locks at most one instance row. The only exception is
   `startChild`, which locks the ancestor first.
5. Inserting a wakeup for another instance takes only `FOR KEY SHARE`. Do it
   only in transactions that hold no other instance row, or the ancestor's.

## Stop rule

If an existing test in `AwaitRaceSuite`, `FirstCompletedSuite`,
`ChildrenSuite` or `InheritanceSuite` can only pass by weakening it, beyond
the documented race-order change, stop that item and report.

## Report

- The ledger rows before and after:
  - event kinds;
  - incarnation markers;
  - "already handled" markers;
  - background passes;
  - tables;
  - places that write wakeups;
  - transactions that take the advisory lock.
- The loop-probe time (`RestartableSuite` loop, n = 20) before and after.
- The deadlock total and the stuck-instance results.
