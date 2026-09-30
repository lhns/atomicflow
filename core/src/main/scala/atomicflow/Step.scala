package atomicflow

import atomicflow.internal.{StepInputFingerprints, StepScope, StepState}

import scala.compiletime.constValue
import scala.util.control.NonFatal

object Step {
  inline def apply[UUID <: String & Singleton, V <: Int & Singleton]: StepBuilder =
    new StepBuilder(StepId(constValue[UUID]), constValue[V].toLong)

  inline def cached[UUID <: String & Singleton, V <: Int & Singleton]: CachedStepBuilder =
    new CachedStepBuilder(StepId(constValue[UUID]), constValue[V].toLong)

  /** An at-most-once step for side effects. It deliberately has no version: once completed it never runs again
    * automatically, and a failed attempt that is retried already runs the currently deployed body. A new side effect
    * should get a new step id. */
  inline def onlyOnce[UUID <: String & Singleton]: OnlyOnceStepBuilder =
    new OnlyOnceStepBuilder(StepId(constValue[UUID]), OnlyOnceStepBuilder.retryAlways)

  inline def awaiting[UUID <: String & Singleton]: AwaitingStepBuilder =
    new AwaitingStepBuilder(StepId(constValue[UUID]), OnlyOnceStepBuilder.retryAlways)
}

/** Pure step builder. The id/version are captured for uniform syntax with cached/onlyOnce
  * and reserved for future observability (tracing, metrics). */
class StepBuilder(id: StepId, version: Long) {
  def apply[Out](body: => Out)(using wfCtx: WorkflowContext): Out = body
}

private[atomicflow] def stepScope(id: StepId, version: Long)(using wfCtx: WorkflowContext): StepScope =
  StepScope(
    StepMeta(
      id = id,
      version = version,
      name = None,
      description = None,
      workflowMeta = wfCtx.meta,
      workflowInstanceId = wfCtx.instanceId
    ),
    wfCtx.workflowScope
  )

/** Commit before observation: a step's caller always observes the value as it comes back from the cache, so the
  * first run and every replay see exactly the same value, even with a lossy [[Cacheable]]. */
private[atomicflow] def observed[Out: Cacheable](value: Out): Out =
  Cacheable[Out].deserialize(Cacheable[Out].serialize(value))

private[atomicflow] def fingerprints(inputs: Seq[StepInput[?]])(using wfCtx: WorkflowContext): StepInputFingerprints = {
  val fingerprinter = wfCtx.getFingerprinter
  StepInputFingerprints(inputs.map(i => i.name -> i.fingerprint(fingerprinter)).toMap)
}

/** The fingerprints of the inputs that identify a step execution, i.e. whose change re-runs the step. */
private[atomicflow] def keyFingerprints(
                                        inputs: Seq[StepInput[?]],
                                        all: StepInputFingerprints,
                                        default: InputPolicy
                                      ): StepInputFingerprints = {
  val keyNames = inputs.filter { input =>
    (if (input.policy == InputPolicy.Default) default else input.policy) == InputPolicy.InvalidateOn
  }.map(_.name).toSet
  StepInputFingerprints(all.fingerprints.filter { case (name, _) => keyNames.contains(name) })
}

/** An at-least-once step: its result is cached per input, and it re-runs when its inputs or its version change.
  * Inputs marked `.ensureUnchanged` instead raise [[WorkflowError.StepConflict]] when they change. */
class CachedStepBuilder(id: StepId, version: Long) {
  def apply[Out: Cacheable](inputs: StepInput[?]*)(body: => Out)(using wfCtx: WorkflowContext): Out = {
    val scope = stepScope(id, version)
    val inputFingerprints = fingerprints(inputs)
    val idempotencyId = wfCtx.getStepIdempotencyStore(scope)
      .acquireStepIdempotencyId(keyFingerprints(inputs, inputFingerprints, InputPolicy.InvalidateOn))
    val cache = wfCtx.getStepCache[Out](scope)
    cache.get(idempotencyId, inputFingerprints) match {
      case StepState.Completed(value) => value
      case StepState.NotStarted | StepState.Started =>
        wfCtx.checkpoint()
        val result = observed(body)
        cache.put(idempotencyId, inputFingerprints, result, wfCtx.defaultCacheTtl)
        result
    }
  }
}

object OnlyOnceStepBuilder {
  val retryAlways: Throwable => Boolean = _ => true
}

/** An at-most-once step. Before its body runs, a durable `Started` marker is written:
  *  - if the body completes, its result is cached and it never runs again;
  *  - if the body throws and `retryIf` accepts the exception, the marker is cleared and a later run retries the step;
  *  - otherwise (or if the process dies mid-step) the marker stays, and later runs fail with
  *    [[WorkflowError.StepUnknownState]] until an operator overrides the step's idempotency id.
  *
  * By default every exception is considered retryable. Use [[strict]] when an exception may occur after the side
  * effect already happened (e.g. a read timeout), or [[retryIf]] to classify exceptions.
  *
  * Inputs must stay unchanged by default ([[WorkflowError.StepConflict]] otherwise). Inputs marked `.invalidateOn`
  * deliberately perform the side effect again when they change. */
class OnlyOnceStepBuilder(id: StepId, shouldRetry: Throwable => Boolean) {
  /** Only exceptions matching `predicate` count as "the side effect did not happen". */
  def retryIf(predicate: Throwable => Boolean): OnlyOnceStepBuilder =
    new OnlyOnceStepBuilder(id, predicate)

  /** No exception counts as "the side effect did not happen": every failure needs an explicit override. */
  def strict: OnlyOnceStepBuilder =
    retryIf(_ => false)

  def apply[Out: Cacheable](inputs: StepInput[?]*)(body: => Out)(using wfCtx: WorkflowContext): Out = {
    val scope = stepScope(id, 0L)
    val inputFingerprints = fingerprints(inputs)
    val idempotencyId = wfCtx.getStepIdempotencyStore(scope)
      .acquireOnlyOnceStepIdempotencyId(keyFingerprints(inputs, inputFingerprints, InputPolicy.EnsureUnchanged))
    val cache = wfCtx.getStepCache[Out](scope)
    cache.get(idempotencyId, inputFingerprints) match {
      case StepState.Completed(value) => value
      case StepState.Started =>
        throw WorkflowError.StepUnknownState(wfCtx.meta, wfCtx.instanceId, scope.stepMeta)
      case StepState.NotStarted =>
        wfCtx.checkpoint()
        cache.markStarted(idempotencyId, inputFingerprints)
        val result =
          try observed(body)
          catch {
            case e: PendingSignal =>
              // Awaiting a signal inside the body is not a side effect failure.
              cache.clearStarted(idempotencyId)
              throw e
            case NonFatal(e) if shouldRetry(e) =>
              cache.clearStarted(idempotencyId)
              throw e
          }
        cache.put(idempotencyId, inputFingerprints, result, wfCtx.defaultCacheTtl)
        result
    }
  }
}

/** Awaiting step: runs `initiate` exactly once (like [[Step.onlyOnce]]), then reads `signal`.
  * If the signal is unset, the run aborts pending ([[WorkflowError.SignalEmpty]]) and
  * a later `recover()` resumes it. Once the signal is observed, its value is captured in the
  * step cache so completed replays no longer depend on the signal row.
  *
  * The inputs apply to both halves with their defaults: a drifting input conflicts on `initiate` (at-most-once) and
  * re-reads the signal for the capture (cached). With inputs that are stable per instance, e.g. a per-attempt child
  * keyed by a business id, neither happens. */
class AwaitingStepBuilder(id: StepId, shouldRetry: Throwable => Boolean) {
  private val captureStepId: StepId = StepId.unsafeMake(
    java.util.UUID.nameUUIDFromBytes(s"${StepId.unwrap(id)}/awaiting".getBytes("UTF-8")).toString
  )

  /** See [[OnlyOnceStepBuilder.retryIf]]; applies to `initiate`. */
  def retryIf(predicate: Throwable => Boolean): AwaitingStepBuilder =
    new AwaitingStepBuilder(id, predicate)

  /** See [[OnlyOnceStepBuilder.strict]]; applies to `initiate`. */
  def strict: AwaitingStepBuilder =
    retryIf(_ => false)

  @throws[WorkflowError.SignalEmpty]
  def apply[A](signal: Signal[A], inputs: StepInput[?]*)(initiate: => Unit)(using wfCtx: WorkflowContext): A = {
    locally {
      import Cacheable.Simple.given
      new OnlyOnceStepBuilder(id, shouldRetry).apply[Unit](inputs*) { initiate }
    }
    given Cacheable[A] = signal.cacheable
    new CachedStepBuilder(captureStepId, 0L).apply[A](inputs*) { signal.value }
  }
}
