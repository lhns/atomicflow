package atomicflow

import scala.compiletime.constValue
import scala.concurrent.duration.FiniteDuration

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
}

object Workflow {
  inline def apply[UUID <: String & Singleton](name: String): WorkflowBuilder =
    new WorkflowBuilder(
      WorkflowId(constValue[UUID]),
      name
    )

}
