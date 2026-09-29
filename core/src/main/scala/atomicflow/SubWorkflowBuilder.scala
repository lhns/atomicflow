package atomicflow

import Cacheable.Simple.given

class SubWorkflowBuilder(id: WorkflowId) {
  def apply[Out](discriminator: String)(body: WorkflowContext ?=> Out)(using parentCtx: WorkflowContext): Out = {
    val childWorkflow = new Workflow[Unit, Out](
      meta = WorkflowMeta(id = id, name = discriminator, description = None),
      body = (ctx, _) => body(using ctx)
    )
    // Deliberately bypasses `run`: pending must keep travelling as a control throwable through the parent body.
    parentCtx.runtime.runWorkflowInstance(childWorkflow, childInstanceId(parentCtx.instanceId, discriminator), (), Constants.defaultCacheTtl, Map.empty)
  }

  /** The instance ID `apply(discriminator)` uses under the given parent instance. */
  def childInstanceId(parentInstanceId: WorkflowInstanceId, discriminator: String): WorkflowInstanceId =
    WorkflowInstanceId.deriveChild(parentInstanceId, id, discriminator)
}
