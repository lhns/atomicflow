package atomicflow

trait WorkflowException {
  self: Exception =>

  def workflowMeta: WorkflowMeta

  def workflowInstanceId: WorkflowInstanceId
}

class WorkflowNotFoundException(
                                 message: String,
                                 override val workflowMeta: WorkflowMeta,
                                 override val workflowInstanceId: WorkflowInstanceId
                               )
  extends Exception(message) with WorkflowException {

  def this()(using workflowCtx: SimpleWorkflowContext) =
    this(
      s"Cannot find workflow instance: $workflowCtx",
      workflowCtx.meta,
      workflowCtx.instanceId
    )
}

class WorkflowLockedException(
                               message: String,
                               override val workflowMeta: WorkflowMeta,
                               override val workflowInstanceId: WorkflowInstanceId
                             )
  extends Exception(message) with WorkflowException {

  def this()(using workflowCtx: SimpleWorkflowContext) =
    this(
      s"Cannot execute locked workflow instance: $workflowCtx",
      workflowCtx.meta,
      workflowCtx.instanceId
    )
}

class WorkflowInputConflictException(
                                      message: String,
                                      override val workflowMeta: WorkflowMeta,
                                      override val workflowInstanceId: WorkflowInstanceId
                                    )
  extends RuntimeException(message) with WorkflowException {

  def this()(using workflowCtx: SimpleWorkflowContext) =
    this(
      s"Cannot re-run workflow instance with different input: $workflowCtx",
      workflowCtx.meta,
      workflowCtx.instanceId
    )
}
