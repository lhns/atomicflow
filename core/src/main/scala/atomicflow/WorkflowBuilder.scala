package atomicflow

class WorkflowBuilder(id: WorkflowId, name: String) {
  def apply[In: Cacheable, Out](body: In => WorkflowContext ?=> Out): Workflow[In, Out] = {
    val workflowMeta = WorkflowMeta(
      id = id,
      name = name,
      description = None
    )

    new Workflow[In, Out](
      meta = workflowMeta,
      body = { (ctx: WorkflowContext, in: In) =>
        body(in)(using ctx)
      }
    )
  }
}
