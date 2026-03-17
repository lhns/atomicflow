package atomicflow

import atomicflow.Fingerprintable.Fingerprint
import atomicflow.internal.{StepCache, StepIdempotencyStore, StepInputFingerprints, StepScope}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.annotation.implicitNotFound

private[atomicflow] trait StepContext[Out] {
  def stepScope: StepScope

  final def meta: StepMeta = stepScope.stepMeta

  def workflowCtx: WorkflowContext

  override lazy val toString: String = s"$workflowCtx/step:${meta.id}${meta.name.fold("")(name => "#" + URLEncoder.encode(name, StandardCharsets.UTF_8))}"

  def fingerprint(inputs: Seq[StepInput[?]]): StepInputFingerprints

  def idempotencyStore: StepIdempotencyStore.Bound

  def cache(using Cacheable[Out]): StepCache.Bound[Out]

  def onComplete(f: Out => Unit): Unit

  def onCompensate(f: => Unit): Unit
}
