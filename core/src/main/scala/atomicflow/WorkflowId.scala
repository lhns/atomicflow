package atomicflow

opaque type WorkflowId = String

object WorkflowId {
  inline def apply(inline s: String): WorkflowId = UUIDMacros.validateUUID(s)
  def unsafeMake(s: String): WorkflowId = s
  def unwrap(id: WorkflowId): String = id
  extension (id: WorkflowId) def value: String = id
}
