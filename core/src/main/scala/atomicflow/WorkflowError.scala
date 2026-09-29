package atomicflow

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.util.control.ControlThrowable

private[atomicflow] object WorkflowErrorMessages {
  def workflowRef(workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"workflow:${workflowMeta.id}#${URLEncoder.encode(workflowMeta.name, StandardCharsets.UTF_8)}/$workflowInstanceId"

  def stepRef(stepMeta: StepMeta): String =
    s"${workflowRef(stepMeta.workflowMeta, stepMeta.workflowInstanceId)}/step:${stepMeta.id}"

  def signalRef(signal: Signal[?], workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"${workflowRef(workflowMeta, workflowInstanceId)}/$signal"
}

/** All errors raised by AtomicFlow. */
sealed trait WorkflowError extends Throwable {
  def workflowMeta: WorkflowMeta

  def workflowInstanceId: WorkflowInstanceId
}

object WorkflowError {
  final case class NotFound(
                             workflowMeta: WorkflowMeta,
                             workflowInstanceId: WorkflowInstanceId
                           ) extends Exception(
    s"Cannot find workflow instance: ${WorkflowErrorMessages.workflowRef(workflowMeta, workflowInstanceId)}"
  ) with WorkflowError

  final case class Locked(
                           workflowMeta: WorkflowMeta,
                           workflowInstanceId: WorkflowInstanceId
                         ) extends Exception(
    s"Cannot execute locked workflow instance: ${WorkflowErrorMessages.workflowRef(workflowMeta, workflowInstanceId)}"
  ) with WorkflowError

  final case class InputConflict(
                                  workflowMeta: WorkflowMeta,
                                  workflowInstanceId: WorkflowInstanceId
                                ) extends Exception(
    s"Cannot re-run workflow instance with different input: ${WorkflowErrorMessages.workflowRef(workflowMeta, workflowInstanceId)}"
  ) with WorkflowError

  final case class StepConflict(
                                 workflowMeta: WorkflowMeta,
                                 workflowInstanceId: WorkflowInstanceId,
                                 stepMeta: StepMeta
                               ) extends Exception(
    s"Cannot re-run step with different input: ${WorkflowErrorMessages.stepRef(stepMeta)}"
  ) with WorkflowError

  /** The workflow is pending: it awaits a signal that is not set yet. Resume it with `recover()` once it is set.
    *
    * This is what callers of `run`/`create`/`recover` observe. Inside workflow bodies, pending travels as the internal
    * control throwable [[PendingSignal]] instead, so `NonFatal`, `Try` and `catch { case e: Exception => }` in business
    * code cannot accidentally swallow it. Use `Workflow.orPending` to handle pending sub-work explicitly. */
  final case class SignalEmpty(
                                workflowMeta: WorkflowMeta,
                                workflowInstanceId: WorkflowInstanceId,
                                signal: Signal[?]
                              ) extends Exception(
    s"Empty signal value: ${WorkflowErrorMessages.signalRef(signal, workflowMeta, workflowInstanceId)}"
  ) with WorkflowError

  final case class SignalConflict(
                                   workflowMeta: WorkflowMeta,
                                   workflowInstanceId: WorkflowInstanceId,
                                   signal: Signal[?]
                                 ) extends Exception(
    s"Cannot change signal value: ${WorkflowErrorMessages.signalRef(signal, workflowMeta, workflowInstanceId)}"
  ) with WorkflowError
}

/** Carries a [[WorkflowError.SignalEmpty]] through workflow bodies without being swallowed by business code.
  *
  * Public entry points (`Workflow.run`/`create`/`recover`) unwrap it into the plain exception, because frameworks such
  * as munit, `Future` and cats-effect treat control throwables as fatal. It must never be raised inside a runtime's
  * effect (e.g. a doobie `ConnectionIO`). */
private[atomicflow] final class PendingSignal(val error: WorkflowError.SignalEmpty) extends ControlThrowable
