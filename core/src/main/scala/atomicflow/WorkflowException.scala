package atomicflow

trait WorkflowException {
  self: Exception =>

  def workflowMeta: WorkflowMeta

  def workflowInstanceId: WorkflowInstanceId
}

object WorkflowException {
  def workflowRef(workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    SimpleWorkflowContext(workflowMeta, workflowInstanceId).toString

  def notFoundMessage(workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"Cannot find workflow instance: ${workflowRef(workflowMeta, workflowInstanceId)}"

  def lockedMessage(workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"Cannot execute locked workflow instance: ${workflowRef(workflowMeta, workflowInstanceId)}"

  def inputConflictMessage(workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"Cannot re-run workflow instance with different input: ${workflowRef(workflowMeta, workflowInstanceId)}"
}

class WorkflowNotFoundException(
                                 override val workflowMeta: WorkflowMeta,
                                 override val workflowInstanceId: WorkflowInstanceId,
                                 message: String
                               )
  extends Exception(message) with WorkflowException {
  def this(workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId) =
    this(
      workflowMeta,
      workflowInstanceId,
      WorkflowException.notFoundMessage(workflowMeta, workflowInstanceId)
    )
}

class WorkflowLockedException(
                               override val workflowMeta: WorkflowMeta,
                               override val workflowInstanceId: WorkflowInstanceId,
                               message: String
                             )
  extends Exception(message) with WorkflowException {
  def this(workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId) =
    this(
      workflowMeta,
      workflowInstanceId,
      WorkflowException.lockedMessage(workflowMeta, workflowInstanceId)
    )
}

class WorkflowInputConflictException(
                                      override val workflowMeta: WorkflowMeta,
                                      override val workflowInstanceId: WorkflowInstanceId,
                                      message: String
                                    )
  extends RuntimeException(message) with WorkflowException {
  def this(workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId) =
    this(
      workflowMeta,
      workflowInstanceId,
      WorkflowException.inputConflictMessage(workflowMeta, workflowInstanceId)
    )
}
