package atomicflow

import atomicflow.Constants.{defaultCacheTtl, defaultSignalTtl}

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

private[atomicflow] case class WorkflowInstanceBuilder[In: Cacheable, Out](
                                                                            workflow: Workflow[In, Out],
                                                                            instanceId: WorkflowInstanceId,
                                                                            defaultCacheTtl: FiniteDuration = defaultCacheTtl,
                                                                            defaultSignalTtl: FiniteDuration = defaultSignalTtl,
                                                                            stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId] = Map.empty
                                                                          ) {
  def withDefaultCacheTtl(ttl: FiniteDuration): WorkflowInstanceBuilder[In, Out] =
    copy(defaultCacheTtl = ttl)

  def withDefaultSignalTtl(ttl: FiniteDuration): WorkflowInstanceBuilder[In, Out] =
    copy(defaultSignalTtl = ttl)

  def overrideStepIdempotencyId(stepId: StepId, stepIdempotencyId: StepIdempotencyId): WorkflowInstanceBuilder[In, Out] =
    copy(stepIdempotencyIdOverrides = stepIdempotencyIdOverrides + (stepId -> stepIdempotencyId))

  @throws[WorkflowError.InputConflict]
  def create(in: In)(using runtime: WorkflowRuntime): Unit =
    runtime.createWorkflowInstance(this, in)

  @throws[WorkflowError.InputConflict]
  inline def create()(using runtime: WorkflowRuntime, ev: Unit =:= In): Unit =
    create(())

  @throws[WorkflowError.InputConflict]
  def run(in: In)(using runtime: WorkflowRuntime): Out =
    runtime.runWorkflowInstance(this, in)

  @throws[WorkflowError.InputConflict]
  inline def run()(using runtime: WorkflowRuntime, ev: Unit =:= In): Out =
    run(())

  @throws[WorkflowError.InputConflict]
  @throws[TimeoutException]
  def runWithTimeout(in: In, timeout: FiniteDuration)(using runtime: WorkflowRuntime): Out =
    ox.timeout(timeout) {
      run(in)
    }

  @throws[WorkflowError.InputConflict]
  @throws[TimeoutException]
  inline def runWithTimeout(timeout: FiniteDuration)(using runtime: WorkflowRuntime, ev: Unit =:= In): Out =
    runWithTimeout((), timeout)

  @throws[WorkflowError.NotFound]
  def recover()(using runtime: WorkflowRuntime): Out =
    runtime.recoverWorkflowInstance(this)

  @throws[WorkflowError.NotFound]
  @throws[TimeoutException]
  def recoverWithTimeout(timeout: FiniteDuration)(using runtime: WorkflowRuntime): Out =
    ox.timeout(timeout) {
      recover()
    }

  @throws[WorkflowError.NotFound]
  @throws[WorkflowError.SignalConflict]
  def setSignalFor[A](ttl: FiniteDuration)(signal: Signal[A], value: A)(using runtime: WorkflowRuntime): Unit =
    runtime.setSignal(signal, value, ttl, workflow.meta, instanceId)

  @throws[WorkflowError.NotFound]
  @throws[WorkflowError.SignalConflict]
  def setSignal[A](signal: Signal[A], value: A)(using runtime: WorkflowRuntime): Unit =
    runtime.setSignal(signal, value, defaultSignalTtl, workflow.meta, instanceId)
}
