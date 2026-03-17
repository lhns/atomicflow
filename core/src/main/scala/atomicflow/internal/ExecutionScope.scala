package atomicflow.internal

import atomicflow.*

case class WorkflowScope(
                          workflowMeta: WorkflowMeta,
                          workflowInstanceId: WorkflowInstanceId
                        )

case class StepScope(
                      stepMeta: StepMeta,
                      workflowExecutionScope: WorkflowScope
                    ) {
  def stepConflictError(): WorkflowError.StepConflict =
    WorkflowError.StepConflict(stepMeta.workflowMeta, stepMeta.workflowInstanceId, stepMeta)
}
