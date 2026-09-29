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
    Workflow.unwrapPending(rt.runWorkflowInstance(this, instanceId, in, cacheTtl, stepIdempotencyIdOverrides))

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
    Workflow.unwrapPending(rt.createWorkflowInstance(this, instanceId, in, cacheTtl, stepIdempotencyIdOverrides))

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
    Workflow.unwrapPending(rt.recoverWorkflowInstance(this, instanceId, cacheTtl, stepIdempotencyIdOverrides))

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
    // Deliberately bypasses `run`: pending must keep travelling as a control throwable through the parent body.
    ctx.runtime.runWorkflowInstance(this, childInstanceId(ctx.instanceId, discriminator), in, Constants.defaultCacheTtl, Map.empty)

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
  inline def apply[UUID <: String & Singleton](name: String): WorkflowBuilder =
    new WorkflowBuilder(
      WorkflowId(constValue[UUID]),
      name
    )

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

  private[atomicflow] inline def unwrapPending[A](inline body: A): A =
    try body
    catch case p: PendingSignal => throw p.error
}
