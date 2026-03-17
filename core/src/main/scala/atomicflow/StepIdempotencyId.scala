package atomicflow

opaque type StepIdempotencyId = String

object StepIdempotencyId {
  inline def apply(inline s: String): StepIdempotencyId = UUIDMacros.validateUUID(s)
  def unsafeMake(s: String): StepIdempotencyId = s
  def unwrap(id: StepIdempotencyId): String = id
  extension (id: StepIdempotencyId) def value: String = id

  def generate(using runtime: WorkflowRuntime): StepIdempotencyId =
    runtime.generateStepIdempotencyId
}
