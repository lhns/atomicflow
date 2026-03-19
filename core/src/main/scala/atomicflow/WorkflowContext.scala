package atomicflow

import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore, StepScope, WorkflowScope}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import scala.annotation.implicitNotFound
import scala.concurrent.duration.FiniteDuration

@implicitNotFound("Cannot be used outside a Workflow definition: `Workflow(...) {  }`\nYou can require a WorkflowContext for the enclosing method by adding a using clause `(using WorkflowContext)` to its definition.")
trait WorkflowContext {
  def meta: WorkflowMeta

  def instanceId: WorkflowInstanceId

  protected[atomicflow] def runtime: WorkflowRuntime

  override lazy val toString: String = s"workflow:${meta.id}#${URLEncoder.encode(meta.name, StandardCharsets.UTF_8)}/$instanceId"

  protected[atomicflow] def getFingerprinter: Fingerprinter

  protected[atomicflow] final def workflowScope: WorkflowScope =
    WorkflowScope(meta, instanceId)

  protected[atomicflow] def getStepIdempotencyStore(stepScope: StepScope): StepIdempotencyStore

  protected[atomicflow] def getStepCache[StepOut: Cacheable](stepScope: StepScope): StepCache[StepOut]

  protected[atomicflow] def getSignalStore: SignalStore

  protected[atomicflow] def defaultCacheTtl: FiniteDuration
}
