package atomicflow

trait StepException
  extends WorkflowException {
  self: Exception =>

  def stepMeta: StepMeta
}

class StepInputConflictException(
                                  message: String,
                                  override val stepMeta: StepMeta
                                )
  extends Exception(message) with StepException {
  override def workflowMeta: WorkflowMeta = stepMeta.workflowMeta

  override def workflowInstanceId: WorkflowInstanceId = stepMeta.workflowInstanceId

  def this()(using stepCtx: StepContext[?]) =
    this(s"Cannot re-run step with different input: $stepCtx", stepCtx.meta)

  def this(stepMeta: StepMeta) =
    this(
      s"Cannot re-run step with different input: workflow:${stepMeta.workflowMeta.id}/${stepMeta.workflowInstanceId}/step:${stepMeta.id}",
      stepMeta
    )
}
