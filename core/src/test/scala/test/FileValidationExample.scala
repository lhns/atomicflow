package test

// Runnable example for the file-validation use-case (intentionally untracked, do not commit).
// Run with: sbt "core/Test/runMain test.FileValidationExample"
//
// Demonstrates:
//  - a parent workflow spawning one child per business entity (file id as discriminator)
//  - blocking steps (download) vs awaiting steps (manual validation, correction round-trip)
//  - a retry loop keyed by domain identity: the corrected revision id keys the next attempt
//  - external actors addressing child instances via Workflow.childInstanceId
//  - pending children not blocking siblings via Workflow.orPending

import atomicflow.{*, given}
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.memory.InMemoryWorkflowRuntime
import cats.syntax.all.*

import scala.concurrent.duration.DurationInt

object FileValidationExample {
  class Service {
    def listFiles(): Seq[String] = {
      println("[service] listing files")
      Seq("invoice.pdf", "report.pdf")
    }

    def download(fileId: String, revision: String): String = {
      println(s"[service] downloading $fileId ($revision)")
      s"data:$fileId:$revision"
    }

    def requestValidation(fileId: String, revision: String, data: String): Unit =
      println(s"[service] validation requested for $fileId ($revision) — waiting for a human verdict")

    def requestCorrection(fileId: String, revision: String): Unit =
      println(s"[service] correction requested for $fileId ($revision) — waiting for a corrected upload")

    def respondSuccess(fileId: String): Unit =
      println(s"[service] SUCCESS response sent for $fileId")
  }

  val verdictSignal = Signal[String](SignalId("41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0301")) // "valid" | "invalid"
  val correctionSignal = Signal[String](SignalId("41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0302")) // corrected revision id

  case class AttemptIn(fileId: String, revision: String)
  given Cacheable[AttemptIn] =
    Cacheable[Seq[String]].imap(s => AttemptIn(s(0), s(1)))(a => Seq(a.fileId, a.revision))

  def main(args: Array[String]): Unit = {
    given WorkflowRuntime = InMemoryWorkflowRuntime()

    val service = new Service

    // One validation attempt for a specific file revision.
    // Returns Some(correctedRevision) on an invalid verdict, None once valid.
    val attemptWorkflow = Workflow["41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0303"]("validation attempt")[AttemptIn, Option[String]] { (in: AttemptIn) =>
      val data = Step.cached["41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0304", 0]("file" -> in.fileId, "revision" -> in.revision) {
        service.download(in.fileId, in.revision)
      }

      val verdict = Step.awaiting["41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0305", 0](verdictSignal, "revision" -> in.revision) {
        service.requestValidation(in.fileId, in.revision, data)
      }

      if (verdict == "valid")
        None
      else
        Some(
          Step.awaiting["41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0306", 0](correctionSignal, "revision" -> in.revision) {
            service.requestCorrection(in.fileId, in.revision)
          }
        )
    }

    // Retry is business logic: loop while attempts come back invalid,
    // each retry keyed by the corrected revision id delivered by the correction signal.
    val fileWorkflow = Workflow["41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0307"]("process file")[String, Unit] { (fileId: String) =>
      var revision = "r0"
      var corrected = attemptWorkflow.runChild(revision, AttemptIn(fileId, revision))
      while (corrected.isDefined) {
        revision = corrected.get
        corrected = attemptWorkflow.runChild(revision, AttemptIn(fileId, revision))
      }
      Step.onlyOnce["41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0308", 0]("file" -> fileId) {
        service.respondSuccess(fileId)
      }
    }

    val allFilesWorkflow = Workflow["41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0309"]("all files")[Unit, Unit] { _ =>
      val files = Step.cached["41b8a71d-53dc-4a5c-9d5d-3d0f5b1f0310", 0]() {
        service.listFiles()
      }
      val pendings = files.flatMap { fileId =>
        Workflow.orPending(fileWorkflow.runChild(fileId, fileId)).left.toOption
      }
      pendings.headOption.foreach(e => throw e)
    }

    val scanKey = "scan-2026-08-03"
    val rootId = allFilesWorkflow.keyedInstanceId(scanKey)

    // External actors (validation UI, upload endpoint) address an attempt instance
    // knowing only the root instance, the file id and the revision.
    def attemptInstance(fileId: String, revision: String): WorkflowInstanceId =
      attemptWorkflow.childInstanceId(fileWorkflow.childInstanceId(rootId, fileId), revision)

    // Simulated human operator delivering verdicts and corrections asynchronously.
    val operator = new Thread(() => {
      def act(delayMillis: Long)(description: String)(action: => Unit): Unit = {
        Thread.sleep(delayMillis)
        println(s"[operator] $description")
        action
      }

      act(500)("invoice.pdf (r0) looks fine — approving") {
        attemptWorkflow.setSignal(attemptInstance("invoice.pdf", "r0"), verdictSignal, "valid")
      }
      act(500)("report.pdf (r0) has errors — rejecting") {
        attemptWorkflow.setSignal(attemptInstance("report.pdf", "r0"), verdictSignal, "invalid")
      }
      act(500)("uploading corrected report.pdf as r1") {
        attemptWorkflow.setSignal(attemptInstance("report.pdf", "r0"), correctionSignal, "r1")
      }
      act(500)("report.pdf (r1) looks fine — approving") {
        attemptWorkflow.setSignal(attemptInstance("report.pdf", "r1"), verdictSignal, "valid")
      }
    })
    operator.start()

    // The driver: keeps recovering the scan until every file completed.
    // In production this would be a crash-loop / scheduler calling recover().
    println(s"[driver] starting scan $scanKey")
    allFilesWorkflow.create(rootId, ())
    allFilesWorkflow.recoverUntilComplete(rootId, pollInterval = 200.millis)
    operator.join()
    println("[driver] scan complete — all files processed")

    // Replay after completion: everything comes from the cache, no service calls run again.
    println("[driver] replaying completed scan (expect no service output)")
    allFilesWorkflow.recover(rootId)
    println("[driver] replay done")
  }
}
