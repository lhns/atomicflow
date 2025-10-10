package atomicflow

import atomicflow.internal.{StepCache, StepIdempotencyStore, StepInputFingerprints}
import StepAuditEvent.{StepCompleted, StepFailed, StepStarted}
import StepAuditOutcome.{Computed, ShortCircuited}

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

object Step {
  def apply[Out](
                  id: StepId,
                  version: Long,
                  name: String | Unit = (),
                  description: String | Unit = ()
                )(
                  body: StepContext[Out] ?=> Out
                )(using workflowCtx: WorkflowContext[?, ?]): Out = {
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

    val stepWorkflowCtx = workflowCtx

    val auditLogger = stepWorkflowCtx.auditLogger
    val startedAtNanos = System.nanoTime()
    val startedAtInstant = Instant.now()
    auditLogger.log(StepStarted(stepMeta, startedAtInstant))

    var outcome: StepAuditOutcome = Computed

    val completeAtomic: AtomicReference[Out => Unit] = AtomicReference[Out => Unit](_ => ())

    given stepCtx: StepContext[Out] = new StepContext[Out] {
      override def meta: StepMeta = stepMeta

      override def workflowCtx: WorkflowContext[?, ?] = stepWorkflowCtx

      override def fingerprint(inputs: Seq[StepInput[?]]): StepInputFingerprints = {
        val fingerprinter = stepWorkflowCtx.getFingerprinter
        StepInputFingerprints(inputs.map { input =>
          input.name -> input.fingerprint(fingerprinter)
        }.toMap)
      }

      override lazy val idempotencyStore: StepIdempotencyStore = stepWorkflowCtx.getStepIdempotencyStore

      override def cache(using Cacheable[Out]): StepCache[Out] = stepWorkflowCtx.getStepCache

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
        outcome = ShortCircuited
        brk.value
      case NonFatal(t) =>
        val duration = FiniteDuration(System.nanoTime() - startedAtNanos, TimeUnit.NANOSECONDS)
        auditLogger.log(StepFailed(stepMeta, Instant.now(), duration, t))
        throw t
    }

    val duration = FiniteDuration(System.nanoTime() - startedAtNanos, TimeUnit.NANOSECONDS)
    auditLogger.log(StepCompleted(stepMeta, Instant.now(), duration, outcome))

    completeAtomic.get()(result)

    result
  }

  final class StepBreak[Out](val value: Out)(using val ctx: StepContext[Out])
    extends RuntimeException(/*message*/ null, /*cause*/ null, /*enableSuppression=*/ false, /*writableStackTrace*/ false)

  inline def meta(using ctx: StepContext[?]): StepMeta = ctx.meta

  inline def compensate(f: => Unit)(using ctx: StepContext[?]): Unit = ctx.onCompensate(f)

  def cacheFor[Out](ttl: FiniteDuration)(stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit = {
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

  def cache[Out](stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit =
    cacheFor[Out](ctx.workflowCtx.defaultCacheTtl)(stepInputs *)

  @throws[StepInputConflictException]
  def onlyOnceFor[Out](ttl: FiniteDuration)(stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit = {
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

  @throws[StepInputConflictException]
  def onlyOnce[Out](stepInputs: StepInput[?]*)(using ctx: StepContext[Out])(using Cacheable[Out]): Unit =
    onlyOnceFor[Out](ctx.workflowCtx.defaultCacheTtl)(stepInputs *)
}
