package atomicflow

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.FiniteDuration

trait Signal[A] {
  def meta: SignalMeta

  def cacheable: Cacheable[A]

  def ttl: FiniteDuration

  def option(using WorkflowContext): Option[A]

  def isDefined(using WorkflowContext): Boolean = option.isDefined

  def isEmpty(using WorkflowContext): Boolean = option.isEmpty

  @throws[WorkflowError.SignalEmpty]
  def value(using workflowCtx: WorkflowContext): A =
    option.getOrElse(throw WorkflowError.SignalEmpty(workflowCtx.meta, workflowCtx.instanceId, this))

  override def toString: String = s"signal:${meta.id}${meta.name.fold("")(name => "#" + URLEncoder.encode(name, StandardCharsets.UTF_8))}"
}

object Signal {
  def apply[A: Cacheable as A](
                                id: SignalId,
                                name: String | Unit = (),
                                description: String | Unit = (),
                                ttl: FiniteDuration = Constants.defaultSignalTtl
                              ): Signal[A] = {
    new Signal[A] {
      override val meta: SignalMeta = SignalMeta(
        id = id,
        name = name match {
          case () => None
          case string: String => Some(string)
        },
        description = description match {
          case () => None
          case string: String => Some(string)
        }
      )

      override val ttl: FiniteDuration = ttl

      override def cacheable: Cacheable[A] = A

      override def option(using workflowCtx: WorkflowContext): Option[A] =
        workflowCtx.getSignalStore.getSignalValue(this)
    }
  }
}
