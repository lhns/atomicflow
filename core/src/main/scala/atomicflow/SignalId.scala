package atomicflow

opaque type SignalId = String

object SignalId {
  inline def apply(inline s: String): SignalId = UUIDMacros.validateUUID(s)
  def unsafeMake(s: String): SignalId = s
  def unwrap(id: SignalId): String = id
  extension (id: SignalId) def value: String = id
}
