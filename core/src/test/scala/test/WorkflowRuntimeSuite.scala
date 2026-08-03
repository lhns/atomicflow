package test

import atomicflow.WorkflowRuntime
import cats.syntax.all.*
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

  test("Awaiting step should initiate once and complete when its signal is set") {
    val initiations = AtomicInteger(0)
    val signal = Signal[String](SignalId("38b7efd2-b590-4891-b7c0-f057beb46488"))

    val workflow = Workflow["b3dce6e1-9d05-442c-ab16-bc40da457a4d"]("awaiting workflow")[Unit, String] { _ =>
      Step.awaiting["497c1d09-277a-46fc-a17c-d044941436a2", 0](signal) {
        initiations.incrementAndGet()
      }
    }

    val instanceId = WorkflowInstanceId.generate
    workflow.create(instanceId)

    intercept[WorkflowError.SignalEmpty] {
      workflow.recover(instanceId)
    }
    intercept[WorkflowError.SignalEmpty] {
      workflow.recover(instanceId)
    }
    assertEquals(initiations.get(), 1)

    workflow.setSignal(instanceId, signal, "ok")

    assertEquals(workflow.recover(instanceId), "ok")
    assertEquals(workflow.recover(instanceId), "ok")
    assertEquals(initiations.get(), 1)
  }

  test("Business-keyed instances should be stable per key") {
    val counter = AtomicInteger(0)

    val workflow = Workflow["776c5159-93be-4605-b31e-2a0f67286cae"]("keyed workflow")[String, Int] { (in: String) =>
      Step.cached["eb58c561-a5f3-4c02-a2e6-551d67bd50b3", 0]("in" -> in) {
        counter.incrementAndGet()
      }
    }

    assertEquals(workflow.keyedInstanceId("k1"), workflow.keyedInstanceId("k1"))
    assertNotEquals(workflow.keyedInstanceId("k1"), workflow.keyedInstanceId("k2"))

    assertEquals(workflow.runKeyed("k1", "a"), 1)
    assertEquals(workflow.runKeyed("k1", "a"), 1)
    assertEquals(counter.get(), 1)

    intercept[WorkflowError.InputConflict] {
      workflow.runKeyed("k1", "b")
    }

    assertEquals(workflow.runKeyed("k2", "a"), 2)
  }

  test("childInstanceId should address a child instance for external signals") {
    val signal = Signal[String](SignalId("c43913ce-0df7-4b8b-a3e0-6a795bde2e14"))

    val childWorkflow = Workflow["6807c449-3417-40bf-b43d-7c086ebf89dc"]("awaiting child")[Unit, String] { _ =>
      signal.value
    }

    val workflow = Workflow["62474e3f-e23f-4235-91f8-7d38e717227d"]("parent of awaiting child")[Unit, String] { _ =>
      childWorkflow.runChild("d")
    }

    val parentId = WorkflowInstanceId.generate

    intercept[WorkflowError.SignalEmpty] {
      workflow.run(parentId)
    }

    childWorkflow.setSignal(childWorkflow.childInstanceId(parentId, "d"), signal, "done")

    assertEquals(workflow.recover(parentId), "done")
  }

  test("File validation: per-file children, awaiting validation, correction-keyed retries") {
    val listCalls = AtomicInteger(0)
    val downloads = AtomicInteger(0)
    val validationRequests = AtomicInteger(0)
    val correctionRequests = AtomicInteger(0)
    val successResponses = AtomicInteger(0)

    object service {
      def listFiles(): Seq[String] = {
        listCalls.incrementAndGet()
        Seq("f1", "f2", "f3")
      }

      def download(fileId: String, revision: String): String = {
        downloads.incrementAndGet()
        s"data:$fileId:$revision"
      }

      def requestValidation(fileId: String, revision: String, data: String): Unit =
        validationRequests.incrementAndGet()

      def requestCorrection(fileId: String, revision: String): Unit =
        correctionRequests.incrementAndGet()

      def respondSuccess(fileId: String): Unit =
        successResponses.incrementAndGet()
    }

    val verdictSignal = Signal[String](SignalId("74c372bf-1169-4bb3-8a5b-144193aee35e")) // "valid" | "invalid"
    val correctionSignal = Signal[String](SignalId("0e7c3d24-25b2-4e42-9e94-21f54feb891d")) // corrected revision id

    case class AttemptIn(fileId: String, revision: String)
    given Cacheable[AttemptIn] =
      Cacheable[Seq[String]].imap(s => AttemptIn(s(0), s(1)))(a => Seq(a.fileId, a.revision))

    // One validation attempt for a specific file revision.
    // Returns Some(correctedRevision) on an invalid verdict, None once valid.
    val attemptWorkflow = Workflow["f5082b97-f0c8-455e-b219-86ec537089ac"]("validation attempt")[AttemptIn, Option[String]] { (in: AttemptIn) =>
      val data = Step.cached["95d643f5-fe27-4268-90c6-0cb8b38cb2c5", 0]("file" -> in.fileId, "revision" -> in.revision) {
        service.download(in.fileId, in.revision)
      }

      val verdict = Step.awaiting["d2250f1a-6e0f-4fba-86dd-9264016c0adb", 0](verdictSignal, "revision" -> in.revision) {
        service.requestValidation(in.fileId, in.revision, data)
      }

      if (verdict == "valid")
        None
      else
        Some(
          Step.awaiting["1cd844ee-b681-4723-a786-adea3dbcc1bf", 0](correctionSignal, "revision" -> in.revision) {
            service.requestCorrection(in.fileId, in.revision)
          }
        )
    }

    // Retry loop keyed by domain identity: the corrected revision id keys the next attempt.
    val fileWorkflow = Workflow["84425d32-231f-4356-88e8-d48e10977ba0"]("process file")[String, Unit] { (fileId: String) =>
      var revision = "r0"
      var corrected = attemptWorkflow.runChild(revision, AttemptIn(fileId, revision))
      while (corrected.isDefined) {
        revision = corrected.get
        corrected = attemptWorkflow.runChild(revision, AttemptIn(fileId, revision))
      }
      Step.onlyOnce["81852774-5e57-4f25-beba-f8f9301fa5b0", 0]("file" -> fileId) {
        service.respondSuccess(fileId)
      }
    }

    val allFilesWorkflow = Workflow["7f9713b8-2a43-4a34-9d57-a7182dc31712"]("all files")[Unit, Unit] { _ =>
      val files = Step.cached["805526f9-e9d7-4f3e-a297-5ad693068598", 0]() {
        service.listFiles()
      }
      val pendings = files.flatMap { fileId =>
        Workflow.orPending(fileWorkflow.runChild(fileId, fileId)).left.toOption
      }
      pendings.headOption.foreach(e => throw e)
    }

    def counters =
      (listCalls.get(), downloads.get(), validationRequests.get(), correctionRequests.get(), successResponses.get())

    val scanKey = "scan-2026-08-03"
    val rootId = allFilesWorkflow.keyedInstanceId(scanKey)

    // External actors address an attempt knowing only root id, file id and revision.
    def attemptInstance(fileId: String, revision: String): WorkflowInstanceId =
      attemptWorkflow.childInstanceId(fileWorkflow.childInstanceId(rootId, fileId), revision)

    // Pass 1: all three files download and request validation, everything pending
    intercept[WorkflowError.SignalEmpty] {
      allFilesWorkflow.runKeyed(scanKey)
    }
    assertEquals(counters, (1, 3, 3, 0, 0))

    // Replay without news: no side effect runs again
    intercept[WorkflowError.SignalEmpty] {
      allFilesWorkflow.recover(rootId)
    }
    assertEquals(counters, (1, 3, 3, 0, 0))

    // f1 is valid — completes even though f2/f3 are still pending
    attemptWorkflow.setSignal(attemptInstance("f1", "r0"), verdictSignal, "valid")
    intercept[WorkflowError.SignalEmpty] {
      allFilesWorkflow.recover(rootId)
    }
    assertEquals(counters, (1, 3, 3, 0, 1))

    // f2 is invalid — a correction is requested
    attemptWorkflow.setSignal(attemptInstance("f2", "r0"), verdictSignal, "invalid")
    intercept[WorkflowError.SignalEmpty] {
      allFilesWorkflow.recover(rootId)
    }
    assertEquals(counters, (1, 3, 3, 1, 1))

    // Verdicts are write-once per attempt
    intercept[WorkflowError.SignalConflict] {
      attemptWorkflow.setSignal(attemptInstance("f2", "r0"), verdictSignal, "valid")
    }

    // The corrected revision r1 arrives and keys a fresh attempt: re-download, re-validate
    attemptWorkflow.setSignal(attemptInstance("f2", "r0"), correctionSignal, "r1")
    intercept[WorkflowError.SignalEmpty] {
      allFilesWorkflow.recover(rootId)
    }
    assertEquals(counters, (1, 4, 4, 1, 1))

    // The fresh attempt has a fresh write-once slot for its verdict
    attemptWorkflow.setSignal(attemptInstance("f2", "r1"), verdictSignal, "valid")
    intercept[WorkflowError.SignalEmpty] {
      allFilesWorkflow.recover(rootId)
    }
    assertEquals(counters, (1, 4, 4, 1, 2))

    // f3 is valid — the whole scan completes
    attemptWorkflow.setSignal(attemptInstance("f3", "r0"), verdictSignal, "valid")
    allFilesWorkflow.recover(rootId)
    assertEquals(counters, (1, 4, 4, 1, 3))

    // Replay after completion changes nothing
    allFilesWorkflow.recover(rootId)
    assertEquals(counters, (1, 4, 4, 1, 3))
  }
}
