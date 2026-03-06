package atomicflow

trait StepException
  extends WorkflowException {
  self: Exception =>

  def stepMeta: StepMeta
}

object StepException {
  def stepRef(stepMeta: StepMeta): String =
    s"${WorkflowException.workflowRef(stepMeta.workflowMeta, stepMeta.workflowInstanceId)}/step:${stepMeta.id}"

  def inputConflictMessage(stepMeta: StepMeta): String =
    s"Cannot re-run step with different input: ${stepRef(stepMeta)}"
}

class StepInputConflictException(
                                  override val stepMeta: StepMeta,
                                  message: String
                                )
  extends Exception(message) with StepException {
  override def workflowMeta: WorkflowMeta = stepMeta.workflowMeta

  override def workflowInstanceId: WorkflowInstanceId = stepMeta.workflowInstanceId

  def this(stepMeta: StepMeta) =
    this(stepMeta, StepException.inputConflictMessage(stepMeta))
}
