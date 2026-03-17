package test

import atomicflow.WorkflowRuntime
import munit.*
import atomicflow.{*, given}
import Cacheable.Simple.given

import java.util.concurrent.atomic.AtomicInteger

abstract class WorkflowRuntimeSuite extends FunSuite {
  def createWorkflowRuntime: WorkflowRuntime

  given WorkflowRuntime = createWorkflowRuntime

  private lazy val emptyWorkflow = Workflow[String, Int](
    WorkflowId("d9ab1884-6e83-48d7-82c4-cbc89fb32ffc"),
    name = "read and send files"
  ) { (string: String) =>
    if (string == "answer")
      42
    else
      0
  }

  test("Unknown empty workflow should fail with WorkflowNotFoundException") {
    intercept[WorkflowError.NotFound] {
      emptyWorkflow.recover(WorkflowInstanceId.generate)
    }
  }

  test("Empty workflow should run") {
    assertEquals(emptyWorkflow.run(WorkflowInstanceId.generate, "answer"), 42)
  }

  test("Locked empty workflow should fail with WorkflowLockedException") {
    val instanceId = WorkflowInstanceId.generate
    lazy val workflow: Workflow[Unit, Any] = Workflow[Unit, Any](
      WorkflowId("d9ab1884-6e83-48d7-82c4-cbc89fb32ffc"),
      name = "recursive workflow"
    ) { _ =>
      intercept[WorkflowError.Locked] {
        workflow.recover(instanceId)
      }
    }
    workflow.run(instanceId, ())
  }

  test("Empty workflow should fail with WorkflowInputConflictException if its inputs change") {
    val workflowInstanceId = WorkflowInstanceId.generate
    assertEquals(emptyWorkflow.run(workflowInstanceId, "answer"), 42)
    intercept[WorkflowError.InputConflict] {
      emptyWorkflow.run(workflowInstanceId, "hello")
    }
  }

  test("Workflow with step should run") {
    val workflow = Workflow[String, Int](
      WorkflowId("15f8bf6d-a719-4958-b790-b3eb846340c1"),
      name = "workflow with steps"
    ) { (string: String) =>
      Step["d27142b9-e7db-4b8e-b341-6dd3009655c7", 0] {
        if (string == "answer")
          42
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.run(workflowInstanceId, "answer"), 42)

    assertEquals(workflow.run(workflowInstanceId, "answer"), 42)
  }

  test("Workflow with cached step should run") {
    val answer = AtomicInteger(42)

    val workflow = Workflow[String, Int](
      WorkflowId("15f8bf6d-a719-4958-b790-b3eb846340c1"),
      name = "workflow with cached steps"
    ) { (string: String) =>
      Step.cached["d27142b9-e7db-4b8e-b341-6dd3009655c7", 0](
        "input" -> string
      ) {
        if (string == "answer")
          answer.getAndIncrement()
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.run(workflowInstanceId, "answer"), 42)

    assertEquals(workflow.run(workflowInstanceId, "answer"), 42)

    assertEquals(workflow.run(WorkflowInstanceId.generate, "answer"), 43)
  }

  test("Workflow with cached step and changed inputs should run") {
    val answer = AtomicInteger(42)

    val workflow = Workflow[String, Int](
      WorkflowId("330be340-1958-427d-8baf-0f7562d53a97"),
      name = "workflow with cached steps, changed inputs"
    ) { (string: String) =>
      val a = Step["0bc4b603-81ec-4af2-a6c5-700df0084243", 0] {
        answer.getAndIncrement()
      }

      Step.cached["d27142b9-e7db-4b8e-b341-6dd3009655c7", 0](
        "input" -> string,
        "a" -> a
      ) {
        if (string == "answer")
          a
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.run(workflowInstanceId, "answer"), 42)

    assertEquals(workflow.run(workflowInstanceId, "answer"), 43)

    assertEquals(workflow.run(WorkflowInstanceId.generate, "answer"), 44)
  }

  test("Workflow with once step should run") {
    val answer = AtomicInteger(42)

    val workflow = Workflow[String, Int](
      WorkflowId("72b29e07-46ad-4b95-b591-cea2a4dbffce"),
      name = "workflow with once steps"
    ) { (string: String) =>
      Step.onlyOnce["d27142b9-e7db-4b8e-b341-6dd3009655c7", 0](
        "input" -> string
      ) {
        if (string == "answer")
          answer.getAndIncrement()
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.run(workflowInstanceId, "answer"), 42)

    assertEquals(workflow.run(workflowInstanceId, "answer"), 42)

    assertEquals(workflow.run(WorkflowInstanceId.generate, "answer"), 43)
  }

  test("Workflow with once step and changed inputs should run") {
    val answer = AtomicInteger(42)

    val workflow = Workflow[String, Int](
      WorkflowId("a8d67a06-d4e2-4d20-8088-002fca20f789"),
      name = "workflow with once steps, changed inputs"
    ) { (string: String) =>
      val a = Step["0bc4b603-81ec-4af2-a6c5-700df0084243", 0] {
        answer.get()
      }

      Step.onlyOnce["d27142b9-e7db-4b8e-b341-6dd3009655c7", 0](
        "input" -> string,
        "a" -> a
      ) {
        if (string == "answer")
          a
        else
          0
      }
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    assertEquals(workflow.run(workflowInstanceId, "answer"), 42)

    answer.incrementAndGet()

    intercept[WorkflowError.StepConflict] {
      workflow.run(workflowInstanceId, "answer")
    }

    assertEquals(
      workflow.run(
        workflowInstanceId,
        "answer",
        stepIdempotencyIdOverrides = Map(
          StepId("d27142b9-e7db-4b8e-b341-6dd3009655c7") -> StepIdempotencyId.generate
        )
      ),
      43
    )

    assertEquals(workflow.run(workflowInstanceId, "answer"), 43)

    answer.incrementAndGet()

    assertEquals(workflow.run(WorkflowInstanceId.generate, "answer"), 44)
  }

  test("Signals can be set but not to a different value") {
    val signal = Signal[String](SignalId("fbf1760e-c3d4-4635-a84b-8426f2d521ef"))

    val workflow = Workflow[Unit, String](
      WorkflowId("9196475c-8973-47d1-be37-dbb080c943b9"),
      name = "workflow with signal"
    ) { _ =>
      val value = signal.value
      value
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    intercept[WorkflowError.SignalEmpty] {
      workflow.run(workflowInstanceId)
    }

    workflow.setSignal(workflowInstanceId, signal, "test")

    assertEquals(workflow.run(workflowInstanceId), "test")

    workflow.setSignal(workflowInstanceId, signal, "test")

    intercept[WorkflowError.SignalConflict] {
      workflow.setSignal(workflowInstanceId, signal, "test2")
    }
  }

  test("Signals must be set on existing workflows") {
    val signal = Signal[String](SignalId("a6d6993a-1d27-43a2-b254-b53d164e2de3"))

    val workflow = Workflow[Unit, String](
      WorkflowId("b62a9a63-b6c6-4c05-bbf4-e994ee195437"),
      name = "workflow with signal"
    ) { _ =>
      val value = signal.value
      value
    }

    val workflowInstanceId = WorkflowInstanceId.generate

    intercept[WorkflowError.NotFound] {
      workflow.setSignal(workflowInstanceId, signal, "test")
    }

    workflow.create(workflowInstanceId)

    workflow.setSignal(workflowInstanceId, signal, "test")
  }
}
