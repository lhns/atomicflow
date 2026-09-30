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

  /** The workflow version (`WorkflowMeta.version`) this instance was created with. */
  protected[atomicflow] def versionAtCreation: Int

  /** The context of the enclosing workflow run when this instance runs as a child. */
  protected[atomicflow] def parent: Option[WorkflowContext]

  /** The top-level instance of the tree this instance belongs to (itself for top-level instances). */
  protected[atomicflow] def rootWorkflowId: WorkflowId

  protected[atomicflow] def rootInstanceId: WorkflowInstanceId

  /** Keeps this instance's execution lock alive; throws [[WorkflowError.Locked]] if it was lost. */
  protected[atomicflow] def renewLock(): Unit = ()

  /** Whether cancellation was requested for this instance (not considering ancestors). */
  protected[atomicflow] def isCancelRequested: Boolean = false

  /** Depth of nested `Workflow.uncancellable` regions in this instance. Workflow bodies run on a single thread. */
  @volatile private[atomicflow] var uncancellableDepth: Int = 0

  private[atomicflow] def isUncancellable: Boolean =
    uncancellableDepth > 0 || parent.exists(_.isUncancellable)

  private[atomicflow] def isCancelled: Boolean =
    isCancelRequested || parent.exists(_.isCancelled)

  private def renewLocks(): Unit = {
    // A parent reaches no checkpoint of its own while its child runs inline.
    parent.foreach(_.renewLocks())
    renewLock()
  }

  /** Called before any uncached (new) work in this instance, never while replaying cached work:
    *  - renews the execution locks of this instance and its ancestors, so a run that lost its lock never starts new
    *    work ([[WorkflowError.Locked]]);
    *  - throws [[WorkflowError.Cancelled]] if this instance or an ancestor was cancelled, unless inside
    *    `Workflow.uncancellable`. Cancellation stays requested: it is raised again at every new checkpoint. */
  protected[atomicflow] final def checkpoint(): Unit = {
    renewLocks()
    if (!isUncancellable && isCancelled) throw WorkflowError.Cancelled(meta, instanceId)
  }
}
