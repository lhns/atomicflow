package atomicflow

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

private[atomicflow] object WorkflowErrorMessages {
  def workflowRef(workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"workflow:${workflowMeta.id}#${URLEncoder.encode(workflowMeta.name, StandardCharsets.UTF_8)}/$workflowInstanceId"

  def stepRef(stepMeta: StepMeta): String =
    s"${workflowRef(stepMeta.workflowMeta, stepMeta.workflowInstanceId)}/step:${stepMeta.id}"

  def signalRef(signal: Signal[?], workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"${workflowRef(workflowMeta, workflowInstanceId)}/$signal"
}

enum WorkflowError(
                    val workflowMeta: WorkflowMeta,
                    val workflowInstanceId: WorkflowInstanceId,
                    message: String
                  ) extends Exception(message) {

  case NotFound(
                 override val workflowMeta: WorkflowMeta,
                 override val workflowInstanceId: WorkflowInstanceId
               ) extends WorkflowError(
    workflowMeta, workflowInstanceId,
    s"Cannot find workflow instance: ${WorkflowErrorMessages.workflowRef(workflowMeta, workflowInstanceId)}"
  )

  case Locked(
               override val workflowMeta: WorkflowMeta,
               override val workflowInstanceId: WorkflowInstanceId
             ) extends WorkflowError(
    workflowMeta, workflowInstanceId,
    s"Cannot execute locked workflow instance: ${WorkflowErrorMessages.workflowRef(workflowMeta, workflowInstanceId)}"
  )

  case InputConflict(
                      override val workflowMeta: WorkflowMeta,
                      override val workflowInstanceId: WorkflowInstanceId
                    ) extends WorkflowError(
    workflowMeta, workflowInstanceId,
    s"Cannot re-run workflow instance with different input: ${WorkflowErrorMessages.workflowRef(workflowMeta, workflowInstanceId)}"
  )

  case StepConflict(
                     override val workflowMeta: WorkflowMeta,
                     override val workflowInstanceId: WorkflowInstanceId,
                     stepMeta: StepMeta
                   ) extends WorkflowError(
    workflowMeta, workflowInstanceId,
    s"Cannot re-run step with different input: ${WorkflowErrorMessages.stepRef(stepMeta)}"
  )

  case SignalEmpty(
                    override val workflowMeta: WorkflowMeta,
                    override val workflowInstanceId: WorkflowInstanceId,
                    signal: Signal[?]
                  ) extends WorkflowError(
    workflowMeta, workflowInstanceId,
    s"Empty signal value: ${WorkflowErrorMessages.signalRef(signal, workflowMeta, workflowInstanceId)}"
  )

  case SignalConflict(
                       override val workflowMeta: WorkflowMeta,
                       override val workflowInstanceId: WorkflowInstanceId,
                       signal: Signal[?]
                     ) extends WorkflowError(
    workflowMeta, workflowInstanceId,
    s"Cannot change signal value: ${WorkflowErrorMessages.signalRef(signal, workflowMeta, workflowInstanceId)}"
  )
}
