package atomicflow

import atomicflow.internal.{StepInputFingerprints, StepScope}

import java.util.concurrent.atomic.AtomicReference
import scala.compiletime.constValue
import scala.concurrent.duration.FiniteDuration

object Step {
  inline def apply[UUID <: String & Singleton, V <: Int & Singleton]: StepBuilder =
    new StepBuilder(StepId.unsafeMake(UUIDMacros.validateUUID(constValue[UUID])), constValue[V].toLong)

  inline def cached[UUID <: String & Singleton, V <: Int & Singleton]: CachedStepBuilder =
    new CachedStepBuilder(StepId.unsafeMake(UUIDMacros.validateUUID(constValue[UUID])), constValue[V].toLong)

  inline def onlyOnce[UUID <: String & Singleton, V <: Int & Singleton]: OnlyOnceStepBuilder =
    new OnlyOnceStepBuilder(StepId.unsafeMake(UUIDMacros.validateUUID(constValue[UUID])), constValue[V].toLong)

  // Legacy apply for migration — kept internal
  private[atomicflow] def apply[Out](
                  id: StepId,
                  version: Long,
                  name: String | Unit = (),
                  description: String | Unit = ()
                )(
                  body: StepContext[Out] ?=> Out
                )(using workflowCtx: WorkflowContext): Out = {
    val stepMeta = StepMeta(
      id = id,
      version = version,
      name = name match {
        case () => None
        case string: String => Some(string)
      },
      description = description match {
        case () => None
        case string: String => Some(string)
      },
      workflowMeta = workflowCtx.meta,
      workflowInstanceId = workflowCtx.instanceId
    )

    runStep(stepMeta, workflowCtx)(body)
  }

  private[atomicflow] def runStep[Out](stepMeta: StepMeta, wfCtx: WorkflowContext)(body: StepContext[Out] ?=> Out): Out = {
    val currentStepScope = StepScope(stepMeta, wfCtx.workflowScope)

    val completeAtomic: AtomicReference[Out => Unit] = AtomicReference[Out => Unit](_ => ())

    given stepCtx: StepContext[Out] = new StepContext[Out] {
      override def stepScope: StepScope = currentStepScope

      override def workflowCtx: WorkflowContext = wfCtx

      override def fingerprint(inputs: Seq[StepInput[?]]): StepInputFingerprints = {
        val fingerprinter = wfCtx.getFingerprinter
        StepInputFingerprints(inputs.map { input =>
          input.name -> input.fingerprint(fingerprinter)
        }.toMap)
      }

      override lazy val idempotencyStore = wfCtx.getStepIdempotencyStore(currentStepScope)

      override def cache(using Cacheable[Out]) = wfCtx.getStepCache(currentStepScope)

      override def onComplete(f: Out => Unit): Unit =
        completeAtomic.updateAndGet(prev => out => {
          prev(out)
          f(out)
        })

      override def onCompensate(f: => Unit): Unit =
        throw new UnsupportedOperationException("step compensation actions are not supported yet")
    }

    val result = try {
      body
    } catch {
      case brk: StepBreak[Out] @unchecked if stepCtx.eq(brk.ctx) =>
        brk.value
    }

    completeAtomic.get()(result)

    result
  }

  private[atomicflow] final class StepBreak[Out](val value: Out)(using val ctx: StepContext[Out])
    extends RuntimeException(/*message*/ null, /*cause*/ null, /*enableSuppression=*/ false, /*writableStackTrace*/ false)

  // Legacy methods for existing Step.cache/Step.onlyOnce pattern
  private[atomicflow] inline def meta(using ctx: StepContext[?]): StepMeta = ctx.meta

  private[atomicflow] inline def compensate(f: => Unit)(using ctx: StepContext[?]): Unit = ctx.onCompensate(f)

  private[atomicflow] def cacheFor[Out](ttl: FiniteDuration)(stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit = {
    val inputFingerprints = ctx.fingerprint(stepInputs)
    val idempotencyId = ctx.idempotencyStore.acquireStepIdempotencyId(inputFingerprints)
    ctx.cache.get(idempotencyId, inputFingerprints) match {
      case Some(value) =>
        throw new StepBreak(value)
      case None =>
        ctx.onComplete { out =>
          ctx.cache.put(idempotencyId, inputFingerprints, out, ttl)
        }
    }
  }

  private[atomicflow] def cache[Out](stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit =
    cacheFor[Out](ctx.workflowCtx.defaultCacheTtl)(stepInputs *)

  @throws[WorkflowError.StepConflict]
  private[atomicflow] def onlyOnceFor[Out](ttl: FiniteDuration)(stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit = {
    val inputFingerprints = ctx.fingerprint(stepInputs)
    val idempotencyId = ctx.idempotencyStore.acquireOnlyOnceStepIdempotencyId()
    ctx.cache.get(idempotencyId, inputFingerprints) match {
      case Some(value) =>
        throw new StepBreak(value)
      case None =>
        ctx.onComplete { out =>
          ctx.cache.put(idempotencyId, inputFingerprints, out, ttl)
        }
    }
  }

  @throws[WorkflowError.StepConflict]
  private[atomicflow] def onlyOnce[Out](stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit =
    onlyOnceFor[Out](ctx.workflowCtx.defaultCacheTtl)(stepInputs *)
}

class StepBuilder(id: StepId, version: Long) {
  def apply[Out](body: => Out)(using workflowCtx: WorkflowContext): Out = {
    val stepMeta = StepMeta(
      id = id,
      version = version,
      name = None,
      description = None,
      workflowMeta = workflowCtx.meta,
      workflowInstanceId = workflowCtx.instanceId
    )

    Step.runStep(stepMeta, workflowCtx) {
      body
    }
  }
}

class CachedStepBuilder(id: StepId, version: Long) {
  def apply[Out: Cacheable](inputs: StepInput[?]*)(body: => Out)(using workflowCtx: WorkflowContext): Out = {
    val stepMeta = StepMeta(
      id = id,
      version = version,
      name = None,
      description = None,
      workflowMeta = workflowCtx.meta,
      workflowInstanceId = workflowCtx.instanceId
    )

    Step.runStep[Out](stepMeta, workflowCtx) {
      Step.cache[Out](inputs *)
      body
    }
  }
}

class OnlyOnceStepBuilder(id: StepId, version: Long) {
  def apply[Out: Cacheable](inputs: StepInput[?]*)(body: => Out)(using workflowCtx: WorkflowContext): Out = {
    val stepMeta = StepMeta(
      id = id,
      version = version,
      name = None,
      description = None,
      workflowMeta = workflowCtx.meta,
      workflowInstanceId = workflowCtx.instanceId
    )

    Step.runStep[Out](stepMeta, workflowCtx) {
      Step.onlyOnce[Out](inputs *)
      body
    }
  }
}
