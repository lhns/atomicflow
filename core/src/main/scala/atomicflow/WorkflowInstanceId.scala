package atomicflow

opaque type WorkflowInstanceId = String

object WorkflowInstanceId {
  inline def apply(inline s: String): WorkflowInstanceId = UUIDMacros.validateUUID(s)
  def unsafeMake(s: String): WorkflowInstanceId = s
  def unwrap(id: WorkflowInstanceId): String = id
  extension (id: WorkflowInstanceId) def value: String = id

  def generate(using runtime: WorkflowRuntime): WorkflowInstanceId =
    runtime.generateWorkflowInstanceId

  /** Deterministic instance ID of a child workflow run under a parent instance.
    * This is the ID `Workflow.runChild`/`Workflow.sub` use, so external actors can
    * address a child instance (e.g. to set its signals) knowing only the parent
    * instance ID, the child workflow ID and the discriminator. */
  def deriveChild(
                   parentInstanceId: WorkflowInstanceId,
                   childWorkflowId: WorkflowId,
                   discriminator: String
                 ): WorkflowInstanceId =
    unsafeMake(
      java.util.UUID.nameUUIDFromBytes(
        s"${parentInstanceId.value}/${childWorkflowId.value}/$discriminator".getBytes("UTF-8")
      ).toString
    )

  /** Deterministic top-level instance ID derived from a business key,
    * so the same key always addresses the same instance. */
  def deriveKeyed(
                   workflowId: WorkflowId,
                   businessKey: String
                 ): WorkflowInstanceId =
    unsafeMake(
      java.util.UUID.nameUUIDFromBytes(
        s"${workflowId.value}/$businessKey".getBytes("UTF-8")
      ).toString
    )
}
