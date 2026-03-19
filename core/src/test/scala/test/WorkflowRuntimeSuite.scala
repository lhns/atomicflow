package test

import atomicflow.WorkflowRuntime
import munit.*
import atomicflow.{*, given}
import Cacheable.Simple.given

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

abstract class WorkflowRuntimeSuite extends FunSuite {
  def createWorkflowRuntime: WorkflowRuntime

  given WorkflowRuntime = createWorkflowRuntime

  private lazy val emptyWorkflow = Workflow["d9ab1884-6e83-48d7-82c4-cbc89fb32ffc"]("read and send files")[String, Int] { (string: String) =>
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
    lazy val workflow: Workflow[Unit, Any] = Workflow["d9ab1884-6e83-48d7-82c4-cbc89fb32ffc"]("recursive workflow")[Unit, Any] { _ =>
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
    val workflow = Workflow["15f8bf6d-a719-4958-b790-b3eb846340c1"]("workflow with steps")[String, Int] { (string: String) =>
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

    val workflow = Workflow["15f8bf6d-a719-4958-b790-b3eb846340c1"]("workflow with cached steps")[String, Int] { (string: String) =>
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

    val workflow = Workflow["330be340-1958-427d-8baf-0f7562d53a97"]("workflow with cached steps, changed inputs")[String, Int] { (string: String) =>
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

    val workflow = Workflow["72b29e07-46ad-4b95-b591-cea2a4dbffce"]("workflow with once steps")[String, Int] { (string: String) =>
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

    val workflow = Workflow["a8d67a06-d4e2-4d20-8088-002fca20f789"]("workflow with once steps, changed inputs")[String, Int] { (string: String) =>
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

    val workflow = Workflow["9196475c-8973-47d1-be37-dbb080c943b9"]("workflow with signal")[Unit, String] { _ =>
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

    val workflow = Workflow["b62a9a63-b6c6-4c05-bbf4-e994ee195437"]("workflow with signal")[Unit, String] { _ =>
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

  test("Loop with cached steps should cache independently per iteration") {
    val counter = AtomicInteger(0)
    val items = List("a", "b", "c")

    val workflow = Workflow["c1a2b3d4-e5f6-7890-abcd-ef1234567890"]("loop cached steps")[Unit, List[Int]] { _ =>
      items.map { item =>
        Step.cached["a1b2c3d4-e5f6-7890-abcd-ef1234567890", 0]("item" -> item) {
          counter.incrementAndGet()
        }
      }
    }

    val instanceId = WorkflowInstanceId.generate

    val result1 = workflow.run(instanceId)
    assertEquals(result1, List(1, 2, 3))
    assertEquals(counter.get(), 3)

    val result2 = workflow.run(instanceId)
    assertEquals(result2, List(1, 2, 3))
    assertEquals(counter.get(), 3)
  }

  test("Workflow.sub in a loop should create independent child workflows") {
    val counter = AtomicInteger(0)
    val items = List("a", "b", "c")

    val workflow = Workflow["d4e5f6a7-b8c9-0123-4567-890abcdef012"]("parent with subs")[Unit, List[Int]] { _ =>
      items.map { item =>
        Workflow.sub["e5f6a7b8-c9d0-1234-5678-90abcdef0123"](item) {
          Step.cached["f6a7b8c9-d0e1-2345-6789-0abcdef01234", 0]("item" -> item) {
            counter.incrementAndGet()
          }
        }
      }
    }

    val instanceId = WorkflowInstanceId.generate

    val result1 = workflow.run(instanceId)
    assertEquals(result1, List(1, 2, 3))
    assertEquals(counter.get(), 3)

    val result2 = workflow.run(instanceId)
    assertEquals(result2, List(1, 2, 3))
    assertEquals(counter.get(), 3)
  }

  test("Workflow.runChild with separately-defined child should work") {
    val counter = AtomicInteger(0)
    val items = List("x", "y", "z")

    val childWorkflow = Workflow["a7b8c9d0-e1f2-3456-7890-abcdef012345"]("child workflow")[String, Int] { (item: String) =>
      Step.cached["b8c9d0e1-f2a3-4567-8901-bcdef0123456", 0]("item" -> item) {
        counter.incrementAndGet()
      }
    }

    val workflow = Workflow["c9d0e1f2-a3b4-5678-9012-cdef01234567"]("parent with runChild")[Unit, List[Int]] { _ =>
      items.map { item =>
        childWorkflow.runChild(item, item)
      }
    }

    val instanceId = WorkflowInstanceId.generate

    val result1 = workflow.run(instanceId)
    assertEquals(result1, List(1, 2, 3))
    assertEquals(counter.get(), 3)

    val result2 = workflow.run(instanceId)
    assertEquals(result2, List(1, 2, 3))
    assertEquals(counter.get(), 3)
  }

  test("recoverUntilComplete should block until signal is set") {
    val signal = Signal[String](SignalId("d0e1f2a3-b4c5-6789-0123-def012345678"))

    val workflow = Workflow["e1f2a3b4-c5d6-7890-1234-ef0123456789"]("signal recover")[Unit, String] { _ =>
      signal.value
    }

    val instanceId = WorkflowInstanceId.generate
    workflow.create(instanceId)

    val thread = new Thread(() => {
      Thread.sleep(300)
      workflow.setSignal(instanceId, signal, "hello")
    })
    thread.start()

    val result = workflow.recoverUntilComplete(instanceId, pollInterval = 100.millis)
    assertEquals(result, "hello")
    thread.join()
  }
}
