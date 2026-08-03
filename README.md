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
  minutes.
- **Awaiting steps** (`Step.awaiting`): for work that takes hours or days (manual approval,
  correction round-trips) and must not hold a thread. An awaiting step initiates a side
  effect **exactly once** and then reads a signal. If the signal is not set yet, the run
  aborts with `WorkflowError.SignalEmpty` and the instance is **pending**: a later
  `recover()` (or `recoverUntilComplete`) replays the workflow and picks up where it left
  off — also across service restarts. Once the signal is observed, its value is captured
  in the step cache, so completed instances replay without depending on the signal row.

```scala
val verdict = Signal[String](SignalId("74c372bf-1169-4bb3-8a5b-144193aee35e"))

val reviewed = Workflow["b3dce6e1-9d05-442c-ab16-bc40da457a4d"]("reviewed")[String, String] { (document: String) =>
  Step.awaiting["497c1d09-277a-46fc-a17c-d044941436a2", 0](verdict, "document" -> document) {
    // runs exactly once, even across recoveries
    requestManualReview(document)
  }
}
```

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

For inline one-off children there is `Workflow.sub["<uuid>"](discriminator) { body }`.
Prefer reusable children via `runChild` whenever the child contains awaiting steps —
external actors need the child `Workflow` handle to call `setSignal`.

A pending child aborts the parent run. To let siblings proceed within the same pass, wrap
child runs in `Workflow.orPending` and rethrow at the end so the parent stays pending:

```scala
val pendings = files.flatMap { file =>
  Workflow.orPending(perFile.runChild(file, file)).left.toOption
}
pendings.headOption.foreach(e => throw e)
```

**Retries are business logic**, not a library feature: key each retry attempt by a domain
identity. For example, a correction round-trip returns a new revision ID via a signal, and
that revision ID becomes the discriminator of the next attempt's child run — no retry
counters needed.

**Determinism rule**: discriminators and child inputs must be pure functions of the
workflow input and previous step outputs. Never derive them from the clock, randomness, or
external reads outside of steps — otherwise replay diverges or fails with input conflicts.

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

  case _: WorkflowError.SignalEmpty =>
    // The instance is pending on an unset signal; recover() it later.
}
```

---

## 📝 **Concepts and Terminology**

- **Atomic Workflow**: A workflow is guaranteed to run atomically — either all steps are successful, or none are.
- **Workflow Instance**: A concrete execution identified by `WorkflowInstanceId` — random (`generate`), business-keyed (`keyedInstanceId`/`runKeyed`), or derived for children (`childInstanceId`/`runChild`).
- **Idempotent Steps**: Step replay behavior is keyed by step metadata and input fingerprints.
- **Only-Once Step**: `Step.onlyOnce[...]` keeps a stable step idempotency identity and conflicts on changed replay inputs.
- **Awaiting Step**: `Step.awaiting[...]` initiates exactly once, then waits on a signal without holding a thread.
- **Pending**: the state of an instance whose run aborted on `WorkflowError.SignalEmpty`; resume with `recover()`.
- **Signals**: Instance-scoped write-once values set by `setSignal`, readable from the workflow body through `signal.value`.

---

## 🛠️ **Advanced Features**

- **Runtime Backends**: Use in-memory runtime for local execution and DB runtime for persistent state.
- **TTLs**: Pass `cacheTtl` to `run`/`recover` and `ttl` to `Signal(...)`; for workflows that live longer than the 30-day defaults (multi-week manual processes), raise both.
- **Only-Once Override**: Pass `stepIdempotencyIdOverrides` to `run`/`recover` when an only-once step intentionally needs a new idempotency identity.
- **Polling Drivers**: `runUntilComplete`/`recoverUntilComplete` block and re-recover on an interval until no signal is missing.

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
