package atomicflow

import Cacheable.Simple.given

class SubWorkflowBuilder(id: WorkflowId) {
  def apply[Out](discriminator: String)(body: WorkflowContext ?=> Out)(using parentCtx: WorkflowContext): Out = {
    val childWorkflow = new Workflow[Unit, Out](
      meta = WorkflowMeta(id = id, name = discriminator, description = None),
      body = (ctx, _) => body(using ctx)
    )
    val childId = WorkflowInstanceId.unsafeMake(
      java.util.UUID.nameUUIDFromBytes(
        s"${parentCtx.instanceId.value}/${id.value}/$discriminator".getBytes("UTF-8")
      ).toString
    )
    childWorkflow.run(childId, ())(using parentCtx.runtime)
  }
}
