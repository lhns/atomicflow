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
import atomicflow.*
import atomicflow.impl.memory.InMemoryWorkflowRuntime
import Cacheable.MsgPack.given
import upickle.default.given

given WorkflowRuntime = InMemoryWorkflowRuntime()
```

### **Define a workflow with step caching**

```scala
val workflow = Workflow[String, Int](
  WorkflowId("15f8bf6d-a719-4958-b790-b3eb846340c1"),
  name = "cached workflow"
) { input =>
  Step(StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7"), version = 1) {
    Step.cache("input" -> input)
    if (input == "answer") 42 else 0
  }
}
```

### **Run and recover by workflow instance id**

```scala
val instanceId = WorkflowInstanceId.generate

val first = workflow.instance(instanceId).run("answer")
val replay = workflow.instance(instanceId).run("answer")

assert(first == 42)
assert(replay == 42)

// Recover previously created instance state
val recovered = workflow.instance(instanceId).recover()
assert(recovered == 42)
```

### **Signals**

```scala
val approval = Signal[String](SignalId("fbf1760e-c3d4-4635-a84b-8426f2d521ef"))

val gated = Workflow[Unit, String](
  WorkflowId("9196475c-8973-47d1-be37-dbb080c943b9"),
  name = "signal gated workflow"
) { _ =>
  approval.value
}

val gatedInstanceId = WorkflowInstanceId.generate
gated.instance(gatedInstanceId).create()
gated.instance(gatedInstanceId).setSignal(approval, "approved")

val result = gated.instance(gatedInstanceId).run()
assert(result == "approved")
```

---

## 🔄 **Retrying Workflows**

Retry using the same workflow instance id. AtomicFlow enforces consistency and raises typed exceptions for conflicts.

```scala
val instanceId = WorkflowInstanceId.generate

try {
  workflow.instance(instanceId).run("answer")
} catch {
  case _: WorkflowLockedException =>
    // Another execution is in progress for this instance.

  case _: WorkflowInputConflictException =>
    // Same instance id used with different workflow input.

  case _: StepInputConflictException =>
    // Replay identity/input mismatch for a step.
}
```

---

## 📝 **Concepts and Terminology**

- **Atomic Workflow**: A workflow is guaranteed to run atomically — either all steps are successful, or none are.
- **Workflow Instance**: A concrete execution identified by `WorkflowInstanceId`.
- **Idempotent Steps**: Step replay behavior is keyed by step metadata and input fingerprints.
- **Only-Once Step**: `Step.onlyOnce(...)` keeps a stable step idempotency identity and conflicts on changed replay inputs.
- **Signals**: Workflow-scoped values set by `setSignal`, readable from workflow body through `signal.value`.

---

## 🛠️ **Advanced Features**

- **Runtime Backends**: Use in-memory runtime for local execution and DB runtime for persistent state.
- **Default TTLs**: Configure instance defaults with `withDefaultCacheTtl(...)` and `withDefaultSignalTtl(...)`.
- **Only-Once Override**: Use `overrideStepIdempotencyId(...)` when an only-once step intentionally needs a new idempotency identity.

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
