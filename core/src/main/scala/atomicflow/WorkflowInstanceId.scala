package atomicflow

opaque type WorkflowInstanceId = String

object WorkflowInstanceId {
  inline def apply(inline s: String): WorkflowInstanceId = UUIDMacros.validateUUID(s)
  def unsafeMake(s: String): WorkflowInstanceId = s
  def unwrap(id: WorkflowInstanceId): String = id
  extension (id: WorkflowInstanceId) def value: String = id

  def generate(using runtime: WorkflowRuntime): WorkflowInstanceId =
    runtime.generateWorkflowInstanceId
}
