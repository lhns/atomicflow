package atomicflow

import java.util.UUID
import scala.annotation.implicitNotFound
import scala.concurrent.duration.FiniteDuration

@implicitNotFound("No WorkflowRuntime available.\nAdd a using clause `(using WorkflowRuntime)` to the definition of the enclosing method.")
/** A workflow runtime. The `parent` of create/run/recover is the context of the enclosing workflow when the instance
  * is run as a child (`runChild` / `Workflow.sub`), and `None` for top-level runs. A child remembers the root instance
  * of its tree at creation. */
trait WorkflowRuntime {
  def generateWorkflowInstanceId: WorkflowInstanceId

  def generateStepIdempotencyId: StepIdempotencyId

  @throws[WorkflowError.InputConflict]
  def createWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    in: In,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
    parent: Option[WorkflowContext]
  ): Unit

  /**
   * - Must lock the workflow while running
   */
  @throws[WorkflowError.InputConflict]
  def runWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    in: In,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
    parent: Option[WorkflowContext]
  ): Out

  /**
   * - Must lock the workflow while running
   * - Must throw a WorkflowError.NotFound
   */
  @throws[WorkflowError.NotFound]
  def recoverWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
    parent: Option[WorkflowContext]
  ): Out

  /** Durably requests cancellation of an instance and wakes up its root.
    * Cancellation is root-oriented: cancelling a root cancels its whole tree. */
  @throws[WorkflowError.NotFound]
  def cancelWorkflowInstance(workflowMeta: WorkflowMeta, instanceId: WorkflowInstanceId): Unit

  /** Claims up to `limit` due wakeups of root instances of the given workflows.
    * A claimed wakeup is postponed, so it fires again if the claimer dies before running the instance. */
  def claimWakeups(workflowIds: Set[WorkflowId], limit: Int): Seq[WorkflowRuntime.Wakeup]

  /** (Re)schedules the wakeup of a root instance after `delay`. */
  def scheduleWakeup(workflowId: WorkflowId, instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit

  /**
   * - Must wake up the root instance of the target instance
   */
  @throws[WorkflowError.NotFound]
  @throws[WorkflowError.SignalConflict]
  def setSignal[A](
                    signal: Signal[A],
                    value: A,
                    ttl: FiniteDuration,
                    workflowMeta: WorkflowMeta,
                    workflowInstanceId: WorkflowInstanceId
                  ): Unit
}

object WorkflowRuntime {
  /** A root instance that is due to run. `attempts` counts consecutive failed runs. */
  case class Wakeup(workflowId: WorkflowId, instanceId: WorkflowInstanceId, attempts: Int)

  /** How a run ended, for wakeup bookkeeping: finished runs (completed, pending or cancelled) need no further wakeup;
    * failed runs are retried with exponential backoff. */
  private[atomicflow] def isFinished(outcome: Option[Throwable]): Boolean = outcome match {
    case None => true
    case Some(_: PendingSignal | _: WorkflowError.SignalEmpty | _: WorkflowError.Cancelled) => true
    case Some(_) => false
  }

  private[atomicflow] val maxRetryDelay: FiniteDuration = FiniteDuration(1, java.util.concurrent.TimeUnit.HOURS)

  private[atomicflow] def retryDelay(base: FiniteDuration, attempts: Int): FiniteDuration =
    if (attempts >= 30) maxRetryDelay else (base * (1L << attempts)).min(maxRetryDelay)

  trait GenerateIds extends WorkflowRuntime {

    override def generateWorkflowInstanceId: WorkflowInstanceId = WorkflowInstanceId.unsafeMake(UUID.randomUUID().toString)

    override def generateStepIdempotencyId: StepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)

  }
}
