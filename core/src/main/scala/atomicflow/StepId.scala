package atomicflow

opaque type StepId = String

object StepId {
  inline def apply(inline s: String): StepId = UUIDMacros.validateUUID(s)
  def unsafeMake(s: String): StepId = s
  def unwrap(id: StepId): String = id
  extension (id: StepId) def value: String = id
}
