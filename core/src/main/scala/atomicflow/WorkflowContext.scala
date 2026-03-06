package atomicflow

import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore, StepScope, WorkflowScope}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.annotation.implicitNotFound
import scala.concurrent.duration.FiniteDuration

trait SimpleWorkflowContext {
  def meta: WorkflowMeta

  def instanceId: WorkflowInstanceId

  override lazy val toString: String = s"workflow:${meta.id}#${URLEncoder.encode(meta.name, StandardCharsets.UTF_8)}/$instanceId"
}

object SimpleWorkflowContext {
  def apply(
             workflowMeta: WorkflowMeta,
             workflowInstanceId: WorkflowInstanceId
           ): SimpleWorkflowContext = new SimpleWorkflowContext {
    override def meta: WorkflowMeta = workflowMeta

    override def instanceId: WorkflowInstanceId = workflowInstanceId
  }

  given (stepCtx: StepContext[?]) => SimpleWorkflowContext = stepCtx.workflowCtx
}

@implicitNotFound("Cannot be used outside a Workflow definition: `Workflow(...) {  }`\nYou can require a WorkflowContext for the enclosing method by adding a using clause `(using WorkflowContext)` to its definition.")
trait WorkflowContext[In, Out] extends SimpleWorkflowContext {
  protected[atomicflow] def getFingerprinter: Fingerprinter

  protected[atomicflow] final def workflowScope: WorkflowScope =
    WorkflowScope(meta, instanceId)

  protected[atomicflow] def getStepIdempotencyStore(stepScope: StepScope): StepIdempotencyStore.Bound

  protected[atomicflow] def getStepCache[StepOut: Cacheable](stepScope: StepScope): StepCache.Bound[StepOut]
  
  protected[atomicflow] def getSignalStore: SignalStore.Bound
  
  protected[atomicflow] def defaultCacheTtl: FiniteDuration
}
