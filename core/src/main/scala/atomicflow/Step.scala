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
