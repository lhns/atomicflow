package atomicflow

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

sealed trait StepAuditEvent {
  def meta: StepMeta
  def timestamp: Instant
}

object StepAuditEvent {
  final case class StepStarted(
    meta: StepMeta,
    timestamp: Instant
  ) extends StepAuditEvent

  final case class StepCompleted(
    meta: StepMeta,
    timestamp: Instant,
    duration: FiniteDuration,
    outcome: StepAuditOutcome
  ) extends StepAuditEvent

  final case class StepFailed(
    meta: StepMeta,
    timestamp: Instant,
    duration: FiniteDuration,
    cause: Throwable
  ) extends StepAuditEvent
}

enum StepAuditOutcome {
  case Computed
  case ShortCircuited
}

trait AuditLogger {
  def log(event: StepAuditEvent): Unit
}

object AuditLogger {
  val console: AuditLogger = new AuditLogger {
    override def log(event: StepAuditEvent): Unit = event match {
      case StepAuditEvent.StepStarted(meta, timestamp) =>
        println(s"[atomicflow][audit] event=step-started time=${timestamp} ${formatMeta(meta)}")
      case StepAuditEvent.StepCompleted(meta, timestamp, duration, outcome) =>
        println(s"[atomicflow][audit] event=step-completed time=${timestamp} duration=${duration.toMillis}ms outcome=${outcome} ${formatMeta(meta)}")
      case StepAuditEvent.StepFailed(meta, timestamp, duration, cause) =>
        println(s"[atomicflow][audit] event=step-failed time=${timestamp} duration=${duration.toMillis}ms error=${cause.getClass.getSimpleName}:${Option(cause.getMessage).getOrElse("")} ${formatMeta(meta)}")
    }

    private def formatMeta(meta: StepMeta): String = {
      val workflowName = meta.workflowMeta.name
      val stepName = meta.name.getOrElse("<unnamed>")
      s"workflow=${meta.workflowMeta.id}(${workflowName}) instance=${meta.workflowInstanceId} step=${meta.id} version=${meta.version} name=\"${stepName}\""
    }
  }
}
