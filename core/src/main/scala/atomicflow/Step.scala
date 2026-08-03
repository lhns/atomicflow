package atomicflow

import atomicflow.internal.{StepInputFingerprints, StepScope}

import scala.compiletime.constValue
import scala.concurrent.duration.FiniteDuration

object Step {
  inline def apply[UUID <: String & Singleton, V <: Int & Singleton]: StepBuilder =
    new StepBuilder(StepId(constValue[UUID]), constValue[V].toLong)

  inline def cached[UUID <: String & Singleton, V <: Int & Singleton]: CachedStepBuilder =
    new CachedStepBuilder(StepId(constValue[UUID]), constValue[V].toLong)

  inline def onlyOnce[UUID <: String & Singleton, V <: Int & Singleton]: OnlyOnceStepBuilder =
    new OnlyOnceStepBuilder(StepId(constValue[UUID]), constValue[V].toLong)

  inline def awaiting[UUID <: String & Singleton, V <: Int & Singleton]: AwaitingStepBuilder =
    new AwaitingStepBuilder(StepId(constValue[UUID]), constValue[V].toLong)
}

/** Pure step builder. The id/version are captured for uniform syntax with cached/onlyOnce
  * and reserved for future observability (tracing, metrics). */
class StepBuilder(id: StepId, version: Long) {
  def apply[Out](body: => Out)(using wfCtx: WorkflowContext): Out = body
}

class CachedStepBuilder(id: StepId, version: Long) {
  def apply[Out: Cacheable](inputs: StepInput[?]*)(body: => Out)(using wfCtx: WorkflowContext): Out = {
    val stepMeta = StepMeta(
      id = id,
      version = version,
      name = None,
      description = None,
      workflowMeta = wfCtx.meta,
      workflowInstanceId = wfCtx.instanceId
    )
    val stepScope = StepScope(stepMeta, wfCtx.workflowScope)
    val fingerprinter = wfCtx.getFingerprinter
    val inputFingerprints = StepInputFingerprints(inputs.map(i => i.name -> i.fingerprint(fingerprinter)).toMap)
    val idempotencyId = wfCtx.getStepIdempotencyStore(stepScope).acquireStepIdempotencyId(inputFingerprints)
    val cache = wfCtx.getStepCache[Out](stepScope)
    cache.get(idempotencyId, inputFingerprints) match {
      case Some(value) => value
      case None =>
        val result = body
        cache.put(idempotencyId, inputFingerprints, result, wfCtx.defaultCacheTtl)
        result
    }
  }
}

class OnlyOnceStepBuilder(id: StepId, version: Long) {
  def apply[Out: Cacheable](inputs: StepInput[?]*)(body: => Out)(using wfCtx: WorkflowContext): Out = {
    val stepMeta = StepMeta(
      id = id,
      version = version,
      name = None,
      description = None,
      workflowMeta = wfCtx.meta,
      workflowInstanceId = wfCtx.instanceId
    )
    val stepScope = StepScope(stepMeta, wfCtx.workflowScope)
    val fingerprinter = wfCtx.getFingerprinter
    val inputFingerprints = StepInputFingerprints(inputs.map(i => i.name -> i.fingerprint(fingerprinter)).toMap)
    val idempotencyId = wfCtx.getStepIdempotencyStore(stepScope).acquireOnlyOnceStepIdempotencyId()
    val cache = wfCtx.getStepCache[Out](stepScope)
    cache.get(idempotencyId, inputFingerprints) match {
      case Some(value) => value
      case None =>
        val result = body
        cache.put(idempotencyId, inputFingerprints, result, wfCtx.defaultCacheTtl)
        result
    }
  }
}

/** Awaiting step: runs `initiate` exactly once (like [[Step.onlyOnce]]), then reads `signal`.
  * If the signal is unset, throws [[WorkflowError.SignalEmpty]] — the run aborts pending and
  * a later `recover()` resumes it. Once the signal is observed, its value is captured in the
  * step cache so completed replays no longer depend on the signal row. */
class AwaitingStepBuilder(id: StepId, version: Long) {
  private val captureStepId: StepId = StepId.unsafeMake(
    java.util.UUID.nameUUIDFromBytes(s"${StepId.unwrap(id)}/awaiting".getBytes("UTF-8")).toString
  )

  @throws[WorkflowError.SignalEmpty]
  def apply[A](signal: Signal[A], inputs: StepInput[?]*)(initiate: => Unit)(using wfCtx: WorkflowContext): A = {
    locally {
      import Cacheable.Simple.given
      new OnlyOnceStepBuilder(id, version).apply[Unit](inputs*) { initiate }
    }
    given Cacheable[A] = signal.cacheable
    new CachedStepBuilder(captureStepId, version).apply[A](inputs*) { signal.value }
  }
}
