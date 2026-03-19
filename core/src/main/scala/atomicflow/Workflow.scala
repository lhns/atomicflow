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
    rt.runWorkflowInstance(this, instanceId, in, cacheTtl, stepIdempotencyIdOverrides)

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
    rt.createWorkflowInstance(this, instanceId, in, cacheTtl, stepIdempotencyIdOverrides)

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
    rt.recoverWorkflowInstance(this, instanceId, cacheTtl, stepIdempotencyIdOverrides)

  @throws[WorkflowError.NotFound]
  @throws[WorkflowError.SignalConflict]
  def setSignal[A](
                    instanceId: WorkflowInstanceId,
                    signal: Signal[A],
                    value: A
                  )(using rt: WorkflowRuntime): Unit =
    rt.setSignal(signal, value, signal.ttl, meta, instanceId)

  def runChild(
    discriminator: String,
    in: In
  )(using ctx: WorkflowContext): Out = {
    val childId = WorkflowInstanceId.unsafeMake(
      java.util.UUID.nameUUIDFromBytes(
        s"${ctx.instanceId.value}/${meta.id.value}/$discriminator".getBytes("UTF-8")
      ).toString
    )
    run(childId, in)(using ctx.runtime)
  }

  inline def runChild(
    discriminator: String
  )(using WorkflowContext, Unit =:= In): Out =
    runChild(discriminator, ())

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
}
