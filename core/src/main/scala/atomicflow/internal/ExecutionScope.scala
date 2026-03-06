package atomicflow.internal

import atomicflow.*

case class WorkflowScope(
                          workflowMeta: WorkflowMeta,
                          workflowInstanceId: WorkflowInstanceId
                        ) {
  def simpleWorkflowContext: SimpleWorkflowContext =
    SimpleWorkflowContext(workflowMeta, workflowInstanceId)
}

case class StepScope(
                      stepMeta: StepMeta,
                      workflowExecutionScope: WorkflowScope
                    ) {
  def stepInputConflictException(): StepInputConflictException =
    new StepInputConflictException(stepMeta)
}