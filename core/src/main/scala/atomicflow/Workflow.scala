package atomicflow

import scala.compiletime.constValue
import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

case class Workflow[In: Cacheable, Out] private[atomicflow](
                                                 meta: WorkflowMeta,
                                                 body: (WorkflowContext, In) => Out
                                               ) {
  private[atomicflow] def instance(instanceId: WorkflowInstanceId): WorkflowInstanceBuilder[In, Out] =
    WorkflowInstanceBuilder(
      this,
      instanceId
    )

  @throws[WorkflowError.InputConflict]
  def run(
           instanceId: WorkflowInstanceId,
           in: In,
           cacheTtl: FiniteDuration = Constants.defaultCacheTtl,
           signalTtl: FiniteDuration = Constants.defaultSignalTtl,
           stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
         )(using WorkflowRuntime): Out =
    WorkflowInstanceBuilder(this, instanceId, cacheTtl, signalTtl, stepIdempotencyIdOverrides).run(in)

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
              signalTtl: FiniteDuration = Constants.defaultSignalTtl,
              stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
            )(using WorkflowRuntime): Unit =
    WorkflowInstanceBuilder(this, instanceId, cacheTtl, signalTtl, stepIdempotencyIdOverrides).create(in)

  @throws[WorkflowError.InputConflict]
  inline def create(
              instanceId: WorkflowInstanceId
            )(using WorkflowRuntime, Unit =:= In): Unit =
    create(instanceId, ())

  @throws[WorkflowError.NotFound]
  def recover(
               instanceId: WorkflowInstanceId,
               cacheTtl: FiniteDuration = Constants.defaultCacheTtl,
               signalTtl: FiniteDuration = Constants.defaultSignalTtl,
               stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
             )(using WorkflowRuntime): Out =
    WorkflowInstanceBuilder(this, instanceId, cacheTtl, signalTtl, stepIdempotencyIdOverrides).recover()

  @throws[WorkflowError.NotFound]
  @throws[WorkflowError.SignalConflict]
  def setSignal[A](
                    instanceId: WorkflowInstanceId,
                    signal: Signal[A],
                    value: A,
                    signalTtl: FiniteDuration = Constants.defaultSignalTtl
                  )(using WorkflowRuntime): Unit =
    WorkflowInstanceBuilder(this, instanceId, defaultSignalTtl = signalTtl).setSignal(signal, value)
}

object Workflow {
  inline def apply[UUID <: String & Singleton, Name <: String & Singleton]: WorkflowBuilder =
    new WorkflowBuilder(
      WorkflowId.unsafeMake(UUIDMacros.validateUUID(constValue[UUID])),
      constValue[Name]
    )

  def apply[In: Cacheable, Out](
                                 id: WorkflowId,
                                 name: String,
                                 description: String | Unit = ()
                               )(
                                 body: In => WorkflowContext ?=> Out
                               ): Workflow[In, Out] = {
    val workflowMeta = WorkflowMeta(
      id = id,
      name = name,
      description = description match {
        case () => None
        case string: String => Some(string)
      }
    )

    new Workflow[In, Out](
      meta = workflowMeta,
      body = { (ctx: WorkflowContext, in: In) =>
        body(in)(using ctx)
      }
    )
  }

  def meta(using ctx: WorkflowContext): WorkflowMeta = ctx.meta

  def instanceId(using ctx: WorkflowContext): WorkflowInstanceId = ctx.instanceId
}
