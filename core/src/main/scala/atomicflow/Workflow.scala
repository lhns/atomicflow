package atomicflow

import scala.compiletime.constValue
import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class Workflow[In: Cacheable, Out] private[atomicflow](
                                                 meta: WorkflowMeta,
                                                 body: (WorkflowContext, In) => Out
                                               ) {
  @throws[WorkflowError.InputConflict]
  def run(
           instanceId: WorkflowInstanceId,
           in: In,
           cacheTtl: FiniteDuration = Constants.defaultCacheTtl,
           stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
         )(using rt: WorkflowRuntime): Out =
    Workflow.unwrapPending(rt.runWorkflowInstance(this, instanceId, in, cacheTtl, stepIdempotencyIdOverrides, None))

  @throws[WorkflowError.InputConflict]
  inline def run(
           instanceId: WorkflowInstanceId
         )(using WorkflowRuntime, Unit =:= In): Out =
    run(instanceId, ())

  @throws[WorkflowError.InputConflict]
  def create(
              instanceId: WorkflowInstanceId,
              in: In,
              cacheTtl: FiniteDuration = Constants.defaultCacheTtl,
              stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
            )(using rt: WorkflowRuntime): Unit =
    Workflow.unwrapPending(rt.createWorkflowInstance(this, instanceId, in, cacheTtl, stepIdempotencyIdOverrides, None))

  @throws[WorkflowError.InputConflict]
  inline def create(
              instanceId: WorkflowInstanceId
            )(using WorkflowRuntime, Unit =:= In): Unit =
    create(instanceId, ())

  @throws[WorkflowError.NotFound]
  def recover(
               instanceId: WorkflowInstanceId,
               cacheTtl: FiniteDuration = Constants.defaultCacheTtl,
               stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
             )(using rt: WorkflowRuntime): Out =
    Workflow.unwrapPending(rt.recoverWorkflowInstance(this, instanceId, cacheTtl, stepIdempotencyIdOverrides, None))

  /** Requests cancellation of an instance: from its next new step on, the instance and all its children raise
    * [[WorkflowError.Cancelled]] (see there). Cancel the root of a tree: a parent that does not catch `Cancelled` around
    * a cancelled child's `runChild` gets cancelled along with it. */
  @throws[WorkflowError.NotFound]
  def cancel(instanceId: WorkflowInstanceId)(using rt: WorkflowRuntime): Unit =
    rt.cancelWorkflowInstance(meta, instanceId)

  @throws[WorkflowError.NotFound]
  @throws[WorkflowError.SignalConflict]
  def setSignal[A](
                    instanceId: WorkflowInstanceId,
                    signal: Signal[A],
                    value: A
                  )(using rt: WorkflowRuntime): Unit =
    rt.setSignal(signal, value, signal.ttl, meta, instanceId)

  /** The instance ID `runChild(discriminator, _)` uses under the given parent instance.
    * External actors use this to target signals of a child instance. */
  def childInstanceId(parentInstanceId: WorkflowInstanceId, discriminator: String): WorkflowInstanceId =
    WorkflowInstanceId.deriveChild(parentInstanceId, meta.id, discriminator)

  /** The instance ID `runKeyed(businessKey, _)` uses. */
  def keyedInstanceId(businessKey: String): WorkflowInstanceId =
    WorkflowInstanceId.deriveKeyed(meta.id, businessKey)

  def runChild(
    discriminator: String,
    in: In
  )(using ctx: WorkflowContext): Out =
    Workflow.runAsChild(this, childInstanceId(ctx.instanceId, discriminator), in)

  inline def runChild(
    discriminator: String
  )(using WorkflowContext, Unit =:= In): Out =
    runChild(discriminator, ())

  /** Runs a top-level instance whose ID is derived from a business key:
    * the same key always addresses the same instance. */
  @throws[WorkflowError.InputConflict]
  def runKeyed(
                businessKey: String,
                in: In,
                cacheTtl: FiniteDuration = Constants.defaultCacheTtl,
                stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
              )(using rt: WorkflowRuntime): Out =
    run(keyedInstanceId(businessKey), in, cacheTtl, stepIdempotencyIdOverrides)

  @throws[WorkflowError.InputConflict]
  inline def runKeyed(
                businessKey: String
              )(using WorkflowRuntime, Unit =:= In): Out =
    runKeyed(businessKey, ())

  @throws[WorkflowError.NotFound]
  def recoverUntilComplete(
    instanceId: WorkflowInstanceId,
    pollInterval: FiniteDuration = 5.seconds,
    maxAttempts: Int = Int.MaxValue,
    cacheTtl: FiniteDuration = Constants.defaultCacheTtl,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
  )(using rt: WorkflowRuntime): Out = {
    var attempts = 0
    while (true) {
      attempts += 1
      try return recover(instanceId, cacheTtl, stepIdempotencyIdOverrides)
      catch {
        case _: WorkflowError.SignalEmpty if attempts < maxAttempts =>
          Thread.sleep(pollInterval.toMillis)
      }
    }
    throw new AssertionError("unreachable")
  }

  @throws[WorkflowError.InputConflict]
  def runUntilComplete(
    instanceId: WorkflowInstanceId,
    in: In,
    pollInterval: FiniteDuration = 5.seconds,
    maxAttempts: Int = Int.MaxValue,
    cacheTtl: FiniteDuration = Constants.defaultCacheTtl,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
  )(using rt: WorkflowRuntime): Out = {
    create(instanceId, in, cacheTtl, stepIdempotencyIdOverrides)
    recoverUntilComplete(instanceId, pollInterval, maxAttempts, cacheTtl, stepIdempotencyIdOverrides)
  }
}

object Workflow {
  /** @param version the version of the workflow's code, see [[versionAtCreation]] */
  inline def apply[UUID <: String & Singleton](name: String, version: Int = 1): WorkflowBuilder =
    new WorkflowBuilder(
      WorkflowId(constValue[UUID]),
      name,
      version
    )

  /** The workflow version that created the current instance. Branch on it to evolve a workflow's code while instances
    * created by older versions are still running, e.g. `if (Workflow.versionAtCreation >= 2) newPath else oldPath`. */
  def versionAtCreation(using ctx: WorkflowContext): Int =
    ctx.versionAtCreation

  inline def sub[UUID <: String & Singleton]: SubWorkflowBuilder =
    new SubWorkflowBuilder(WorkflowId(constValue[UUID]))

  /** Runs `body`; a pending abort (a [[WorkflowError.SignalEmpty]] from the subtree —
    * typically a child run awaiting a signal) becomes a `Left` instead of propagating,
    * so sibling work can proceed. Rethrow a collected `Left` with [[pending]] at the end
    * of the parent body to keep the parent itself pending. */
  def orPending[A](body: => A): Either[WorkflowError.SignalEmpty, A] =
    try Right(body)
    catch {
      case p: PendingSignal => Left(p.error)
      case e: WorkflowError.SignalEmpty => Left(e)
    }

  /** Aborts the current run as pending, e.g. with a pending result collected by [[orPending]].
    * Unlike `throw e`, business code catching `NonFatal` cannot swallow it. */
  def pending(e: WorkflowError.SignalEmpty): Nothing =
    throw PendingSignal(e)

  /** Runs `body` without being interrupted by cancellation, e.g. compensating steps after catching
    * [[WorkflowError.Cancelled]]. Applies to child runs started inside `body` as well. */
  def uncancellable[R](body: => R)(using ctx: WorkflowContext): R = {
    ctx.uncancellableDepth += 1
    try body
    finally ctx.uncancellableDepth -= 1
  }

  /** The cache TTL of the current run (inherited by child runs). */
  def cacheTtl(using ctx: WorkflowContext): FiniteDuration =
    ctx.defaultCacheTtl

  /** Runs a child instance in the current context.
    *
    * It deliberately bypasses `run`: pending must keep travelling as a control throwable through the parent body.
    * The child inherits the cache TTL, but not `stepIdempotencyIdOverrides`: an override id is globally unique, so
    * applying one to several child instances would make them collide. */
  private[atomicflow] def runAsChild[In: Cacheable, Out](
                                                         workflow: Workflow[In, Out],
                                                         instanceId: WorkflowInstanceId,
                                                         in: In
                                                       )(using ctx: WorkflowContext): Out =
    ctx.runtime.runWorkflowInstance(workflow, instanceId, in, ctx.defaultCacheTtl, Map.empty, Some(ctx))

  private[atomicflow] inline def unwrapPending[A](inline body: A): A =
    try body
    catch case p: PendingSignal => throw p.error
}
