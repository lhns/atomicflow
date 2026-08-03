package atomicflow

import Cacheable.Simple.given

class SubWorkflowBuilder(id: WorkflowId) {
  def apply[Out](discriminator: String)(body: WorkflowContext ?=> Out)(using parentCtx: WorkflowContext): Out = {
    val childWorkflow = new Workflow[Unit, Out](
      meta = WorkflowMeta(id = id, name = discriminator, description = None),
      body = (ctx, _) => body(using ctx)
    )
    childWorkflow.run(childInstanceId(parentCtx.instanceId, discriminator), ())(using parentCtx.runtime)
  }

  /** The instance ID `apply(discriminator)` uses under the given parent instance. */
  def childInstanceId(parentInstanceId: WorkflowInstanceId, discriminator: String): WorkflowInstanceId =
    WorkflowInstanceId.deriveChild(parentInstanceId, id, discriminator)
}
