package atomicflow.internal

import atomicflow.*

import scala.concurrent.duration.Duration

case class WorkflowScope(
                          workflowMeta: WorkflowMeta,
                          workflowInstanceId: WorkflowInstanceId
                        ) {
  def simpleWorkflowContext: SimpleWorkflowContext =
    SimpleWorkflowContext(workflowMeta, workflowInstanceId)
}

case class StepScope(
                      stepMeta: StepMeta,
                      workflowExecutionScope: WorkflowScope
                    ) {
  def stepInputConflictException(): StepInputConflictException = {
    given StepContext[Any] = diagnosticStepContext
    StepInputConflictException()
  }

  private lazy val diagnosticWorkflowContext: WorkflowContext[Any, Any] = new WorkflowContext[Any, Any] {
    override def meta: WorkflowMeta = workflowExecutionScope.workflowMeta

    override def instanceId: WorkflowInstanceId = workflowExecutionScope.workflowInstanceId

    override protected[atomicflow] def getFingerprinter =
      throw new UnsupportedOperationException("Diagnostic-only context")

    override protected[atomicflow] def getStepIdempotencyStore(stepScope: StepScope): StepIdempotencyStore.Bound =
      throw new UnsupportedOperationException("Diagnostic-only context")

    override protected[atomicflow] def getStepCache[StepOut: Cacheable](stepScope: StepScope): StepCache.Bound[StepOut] =
      throw new UnsupportedOperationException("Diagnostic-only context")

    override protected[atomicflow] def getSignalStore: SignalStore.Bound =
      throw new UnsupportedOperationException("Diagnostic-only context")

    override protected[atomicflow] val defaultCacheTtl =
      Duration.Zero
  }

  private lazy val diagnosticStepContext: StepContext[Any] = new StepContext[Any] {
    override def stepScope: StepScope = StepScope.this

    override def workflowCtx: WorkflowContext[?, ?] = diagnosticWorkflowContext

    override def fingerprint(inputs: Seq[StepInput[?]]): StepInputFingerprints =
      throw new UnsupportedOperationException("Diagnostic-only context")

    override def idempotencyStore: StepIdempotencyStore.Bound =
      throw new UnsupportedOperationException("Diagnostic-only context")

    override def cache(using Cacheable[Any]): StepCache.Bound[Any] =
      throw new UnsupportedOperationException("Diagnostic-only context")

    override def onComplete(f: Any => Unit): Unit =
      throw new UnsupportedOperationException("Diagnostic-only context")

    override def onCompensate(f: => Unit): Unit =
      throw new UnsupportedOperationException("Diagnostic-only context")
  }
}