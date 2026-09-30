# **AtomicFlow - A Workflow Framework with Idempotent Steps and Atomic Replay**

**AtomicFlow** is a workflow framework designed to help you manage and execute workflows in an atomic, idempotent, and repeatable manner. With the ability to handle side effects, deduplication, and easy retries, this framework ensures that business processes are executed reliably while maintaining consistency.

Note: `db/test` requires `DB_URL`, `DB_USERNAME`, and `DB_PASSWORD`; when they are not set, the DB suite is skipped.

## 🧭 **Architecture Decisions**

Architecture decisions are tracked in [adr/README.md](adr/README.md).
Decision relationships between ADRs are documented in [Decision Relationships](adr/README.md#decision-relationships).

---

## 🚀 **Features**

- **Atomic Workflows**: Your workflows are guaranteed to run atomically, meaning either all steps complete successfully, or none of them do.
- **Idempotent Execution**: Steps are idempotent, so re-executing a step with the same inputs and context will not result in side effects being performed multiple times.
- **Only-Once Effects**: Critical steps that have side effects (e.g., sending data to a customer) are protected by **only-once execution** to ensure they are only executed once, unless explicitly retried with a new step idempotency id.
- **Replayable Step State**: Step cache and idempotency state are persisted by the runtime, enabling replay/recovery without re-running already completed work.
- **Replayable Workflows**: If a failure occurs, you can replay the workflow from the beginning, skipping already successfully completed steps.
- **TTL Metadata**: Cache and signal writes support TTL metadata (strict expiry enforcement is runtime-dependent).

---

## Out of scope
- Introspection / Diagram generation

---

## ⚙️ **How it Works**

AtomicFlow focuses on **workflow steps** that can be executed safely and deterministically. Each step can be classified into two types:

1. **Pure Steps** – These steps are computationally simple and are guaranteed to have no side effects. They can be re-executed as needed.
2. **Side-Effecting Steps** – These steps may trigger important, irreversible changes (e.g., sending data to an external service). AtomicFlow ensures these steps are executed **exactly once** using step idempotency identities and input fingerprints.

The framework also supports **retrying workflows** when exceptions occur, ensuring **atomicity** even in case of transient errors.

---

## 📦 **Installation**

1. Add the following to your `build.sbt` (for Scala projects using sbt):

   ```scala
   libraryDependencies += "de.lhns" %% "atomicflow" % "0.0.1"
   ```

2. Alternatively, download and include the jar in your project.

---

## ✅ **Running Tests**

- Run all core tests:

  ```bash
  sbt core/test
  ```

- Run DB tests:

  ```bash
  sbt db/test
  ```

DB tests require the following environment variables:

- `DB_URL`
- `DB_USERNAME`
- `DB_PASSWORD`

If one or more variables are missing, the DB suite is ignored (skipped) instead of failing at initialization.

---

## 🧰 **Example Usage**

### **Set up a runtime**

```scala
import atomicflow.{*, given}
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.memory.InMemoryWorkflowRuntime

given WorkflowRuntime = InMemoryWorkflowRuntime()
```

### **Define a workflow with step caching**

Workflow and step definitions are identified by UUID literals validated at compile time:

```scala
val workflow = Workflow["15f8bf6d-a719-4958-b790-b3eb846340c1"]("cached workflow")[String, Int] { (input: String) =>
  Step.cached["d27142b9-e7db-4b8e-b341-6dd3009655c7", 0]("input" -> input) {
    if (input == "answer") 42 else 0
  }
}
```

### **Run and recover by workflow instance id**

```scala
val instanceId = WorkflowInstanceId.generate

val first = workflow.run(instanceId, "answer")
val replay = workflow.run(instanceId, "answer")

assert(first == 42)
assert(replay == 42)

// Recover previously created instance state (the input is persisted)
val recovered = workflow.recover(instanceId)
assert(recovered == 42)
```

Instance IDs can also be derived deterministically from a business key, so the same key
always addresses the same instance:

```scala
val result = workflow.runKeyed("order-42", "answer")
val sameInstance = workflow.keyedInstanceId("order-42")
```

### **Signals**

```scala
val approval = Signal[String](SignalId("fbf1760e-c3d4-4635-a84b-8426f2d521ef"))

val gated = Workflow["9196475c-8973-47d1-be37-dbb080c943b9"]("signal gated workflow")[Unit, String] { _ =>
  approval.value
}

val gatedInstanceId = WorkflowInstanceId.generate
gated.create(gatedInstanceId)
gated.setSignal(gatedInstanceId, approval, "approved")

val result = gated.run(gatedInstanceId)
assert(result == "approved")
```

Signals are write-once per instance: setting the same value again is a no-op, a different
value throws `WorkflowError.SignalConflict`.

---

## ⏳ **Blocking vs. Awaiting: two kinds of long-running work**

AtomicFlow is direct-style, and long-running work falls into two categories with different
mechanics:

- **Blocking steps** (`Step`, `Step.cached`, `Step.onlyOnce`): the step body simply holds
  the thread until it is done. Use these for work you are willing to block on — seconds to
  minutes. A single step body must finish within the runtime's lock timeout (DB runtime:
  `DbConfig.lockTimeout`, 5 minutes by default); the lock is renewed between steps.
- **Awaiting steps** (`Step.awaiting`): for work that takes hours or days (manual approval,
  correction round-trips) and must not hold a thread. An awaiting step initiates a side
  effect **at most once** and then reads a signal. If the signal is not set yet, the run
  aborts and the instance is **pending**: a later `recover()` — typically triggered by a
  worker when the signal is set (see *Running workflows*) — replays the
  workflow and picks up where it left off, also across service restarts. Once the signal is
  observed, its value is captured in the step cache, so completed instances replay without
  depending on the signal row.

```scala
val verdict = Signal[String](SignalId("74c372bf-1169-4bb3-8a5b-144193aee35e"))

val reviewed = Workflow["b3dce6e1-9d05-442c-ab16-bc40da457a4d"]("reviewed")[String, String] { (document: String) =>
  Step.awaiting["497c1d09-277a-46fc-a17c-d044941436a2"](verdict, "document" -> document) {
    // runs at most once, even across recoveries
    requestManualReview(document)
  }
}
```

Pending cannot be swallowed by accident: inside workflow bodies it travels as a control
throwable, so `Try`, `NonFatal` or `catch { case e: Exception => }` in business code let it
pass. Callers of `run`/`recover` see a plain `WorkflowError.SignalEmpty`.

### **At-most-once steps**

`Step.onlyOnce` (and the `initiate` part of `Step.awaiting`) writes a durable *Started*
marker before its body runs:

- if the body completes, its result is cached and it never runs again;
- if the process dies mid-step, later runs fail with `WorkflowError.StepUnknownState`: the
  side effect may or may not have happened. Check the external system, then override the
  step's idempotency id (`stepIdempotencyIdOverrides`) to run it again;
- if the body **throws**, a flag decides whether the side effect is known not to have
  happened:

```scala
Step.onlyOnce["…"](inputs*) { send() }                  // default: any exception => retry later
Step.onlyOnce["…"].strict(inputs*) { send() }           // any exception => unknown state
Step.onlyOnce["…"].retryIf {                            // classify
  case _: java.net.ConnectException => true              //   nothing was sent: retry
  case _ => false                                         //   e.g. read timeout: unknown
}(inputs*) { send() }
```

At-most-once steps have no version: a completed one never runs again automatically, and a
retried one runs the currently deployed body. A new side effect gets a new step id.

---

## 🧩 **Sub-workflows and dynamic business IDs**

Workflow and step IDs are static UUIDs in the code; *instances* are keyed dynamically.
A workflow can run another workflow as a **child**, using a business ID (file name, order
ID, revision, …) as the discriminator:

```scala
val perFile = Workflow["a7b8c9d0-e1f2-3456-7890-abcdef012345"]("per file")[String, Int] { (file: String) =>
  Step.cached["b8c9d0e1-f2a3-4567-8901-bcdef0123456", 0]("file" -> file) { process(file) }
}

val all = Workflow["c9d0e1f2-a3b4-5678-9012-cdef01234567"]("all files")[Unit, List[Int]] { _ =>
  files.map { file => perFile.runChild(file, file) }
}
```

The child instance ID is derived deterministically from
`(parent instance ID, child workflow ID, discriminator)`, so:

- every loop iteration gets its own scoped instance with its own step cache — step UUIDs
  can be reused across iterations without collision;
- replaying the parent replays the children from their caches;
- derivations chain through arbitrary nesting depth;
- external actors can address a child instance without ever having stored its ID:

```scala
val childId = perFile.childInstanceId(parentInstanceId, "invoice.pdf")
perFile.setSignal(childId, someSignal, "approved")
```

Child runs inherit the parent's cache TTL, but not its `stepIdempotencyIdOverrides` (an
override id is globally unique and cannot be shared by several child instances).

For inline one-off children there is `Workflow.sub["<uuid>"](discriminator) { body }`.
Prefer reusable children via `runChild` whenever the child contains awaiting steps —
external actors need the child `Workflow` handle to call `setSignal`.

A pending child aborts the parent run. To let siblings proceed within the same pass, wrap
child runs in `Workflow.orPending` and rethrow with `Workflow.pending` at the end so the
parent stays pending:

```scala
val pendings = files.flatMap { file =>
  Workflow.orPending(perFile.runChild(file, file)).left.toOption
}
pendings.headOption.foreach(Workflow.pending)
```

**Retries are business logic**, not a library feature: key each retry attempt by a domain
identity. For example, a correction round-trip returns a new revision ID via a signal, and
that revision ID becomes the discriminator of the next attempt's child run — no retry
counters needed.

**Determinism rule**: discriminators and child inputs must be pure functions of the
workflow input and previous step outputs. Never derive them from the clock, randomness, or
external reads outside of steps — otherwise replay diverges or fails with input conflicts.

---

## ▶️ **Running workflows: the worker**

A `WorkflowWorker` runs root instances whenever they are woken up — on creation, whenever a
signal is set anywhere in their tree, and for retries — so nobody has to poll `recover()`:

```scala
val worker = WorkflowWorker(Seq(allFilesWorkflow))  // register root workflows only
val running = worker.start(pollInterval = 1.second)  // daemon thread; or call worker.runOnce()

allFilesWorkflow.create(allFilesWorkflow.keyedInstanceId("scan-2026-09-30"), ())
// ... later, from the validation UI:
attemptWorkflow.setSignal(attemptInstanceId, verdictSignal, "valid")  // wakes the root
```

- Children need no registration: they run inline in their root's pass.
- Failed runs are retried with exponential backoff (`retryBackoff`, capped at one hour).
- A run interrupted by a dead process is retried once its lock has expired.
- Several workers — also in different services with different registrations — can share
  one database; each only claims wakeups of its own workflows (`FOR UPDATE SKIP LOCKED`).
- Evaluation stays self-sufficient: everything a worker does is plain `recover()`, so tests
  can drive workflows without one.

---

## 🧬 **Evolving workflows**

- **Workflow versions:** `Workflow["…"]("name", version = 2)` records the version with every
  new instance. Branch on `Workflow.versionAtCreation` to keep in-flight instances created by
  older code on their old path:

  ```scala
  if (Workflow.versionAtCreation >= 2) newPath() else oldPath()
  ```

- **Cached steps:** keep the version for a compatible fix (cached results are reused), bump
  it to recompute.
- **Input drift policies:** decide per input what a change means:

  ```scala
  Step.cached["…", 0]("file" -> file, ("config" -> config).ensureUnchanged) { … }  // config change => StepConflict
  Step.onlyOnce["…"](("content" -> content).invalidateOn) { send(content) }       // content change => send again
  ```

  Defaults: cached steps invalidate on any input change; at-most-once steps require
  unchanged inputs.
- **Cached formats:** `newFormat.withFallback(oldFormat)` keeps reading values written in an
  older serialization format.
- **Commit before observation:** a step's first run returns its result as it comes back from
  the cache, so the first run and every replay observe exactly the same value.

---

## 🛑 **Cancellation**

`workflow.cancel(instanceId)` durably requests cancellation and wakes the root. From then on,
every *new* step (never a replayed one) raises `WorkflowError.Cancelled`, in the instance and
in all its children, until the workflow finishes. Catch it to compensate:

```scala
try Step.awaiting["…"](approval) { requestApproval() }
catch {
  case _: WorkflowError.Cancelled =>
    Workflow.uncancellable {
      Step.onlyOnce["…"]() { withdrawApprovalRequest() }
    }
    "withdrawn"
}
```

Cancellation is root-oriented: cancel the root of a tree. A cancelled child raises
`Cancelled` through `runChild` into its parent, so a parent that should survive the
cancellation of one child must catch it around that `runChild`.

---

## 🔄 **Retrying Workflows**

Retry using the same workflow instance id. AtomicFlow enforces consistency and raises the
sealed `WorkflowError` for conflicts:

```scala
val instanceId = WorkflowInstanceId.generate

try {
  workflow.run(instanceId, "answer")
} catch {
  case _: WorkflowError.Locked =>
    // Another execution is in progress for this instance.

  case _: WorkflowError.InputConflict =>
    // Same instance id used with different workflow input.

  case _: WorkflowError.StepConflict =>
    // Replay identity/input mismatch for a step.

  case _: WorkflowError.StepUnknownState =>
    // An at-most-once step started but recorded no outcome; needs an operator decision.

  case _: WorkflowError.SignalEmpty =>
    // The instance is pending on an unset signal; setting it wakes the instance up.

  case _: WorkflowError.Cancelled =>
    // The instance was cancelled and did not compensate.
}
```

---

## 📝 **Concepts and Terminology**

- **Atomic Workflow**: A workflow is guaranteed to run atomically — either all steps are successful, or none are.
- **Workflow Instance**: A concrete execution identified by `WorkflowInstanceId` — random (`generate`), business-keyed (`keyedInstanceId`/`runKeyed`), or derived for children (`childInstanceId`/`runChild`).
- **Idempotent Steps**: Step replay behavior is keyed by step metadata and input fingerprints.
- **Only-Once Step**: `Step.onlyOnce[...]` runs a side effect at most once; a crash mid-step leads to `StepUnknownState` instead of a duplicate.
- **Awaiting Step**: `Step.awaiting[...]` initiates at most once, then waits on a signal without holding a thread.
- **Pending**: the state of an instance whose run aborted on an unset signal; setting the signal wakes it up.
- **Root**: the top-level instance of a tree of child runs; wakeups and cancellation target roots.
- **Worker**: `WorkflowWorker` runs woken-up root instances.
- **Signals**: Instance-scoped write-once values set by `setSignal`, readable from the workflow body through `signal.value`.

---

## 🛠️ **Advanced Features**

- **Runtime Backends**: Use in-memory runtime for local execution and DB runtime for persistent state.
- **TTLs**: Pass `cacheTtl` to `run`/`recover` and `ttl` to `Signal(...)`; for workflows that live longer than the 30-day defaults (multi-week manual processes), raise both.
- **Only-Once Override**: Pass `stepIdempotencyIdOverrides` to `run`/`recover` when an only-once step intentionally needs a new idempotency identity.
- **Execution lock**: runs of one instance are mutually exclusive. The DB runtime's lock (`DbConfig.lockTimeout`) is renewed between steps, and step results are only written while the run still owns it.
- **Retry backoff**: `DbConfig.retryBackoff` / `InMemoryWorkflowRuntime(retryBackoff)` sets the base delay for retrying failed runs.
- **Polling Drivers**: `runUntilComplete`/`recoverUntilComplete` block and re-recover on an interval until no signal is missing; prefer a `WorkflowWorker`.

---

## 📚 **Contributing**

We welcome contributions to **AtomicFlow**! If you have ideas for new features, improvements, or bug fixes, feel free to open an issue or submit a pull request.

1. Fork the repo.
2. Create your feature branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-name`).
5. Open a pull request.

---

## 📄 **License**

This project uses the Apache 2.0 License. See the file called LICENSE.
