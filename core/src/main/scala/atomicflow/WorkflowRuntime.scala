package atomicflow

import java.util.UUID
import scala.annotation.implicitNotFound
import scala.concurrent.duration.FiniteDuration

@implicitNotFound("No WorkflowRuntime available.\nAdd a using clause `(using WorkflowRuntime)` to the definition of the enclosing method.")
trait WorkflowRuntime {
  def generateWorkflowInstanceId: WorkflowInstanceId

  def generateStepIdempotencyId: StepIdempotencyId

  @throws[WorkflowError.InputConflict]
  def createWorkflowInstance[In, Out](
                                       workflowInstance: WorkflowInstanceBuilder[In, Out],
                                       in: In
                                     )(
                                       using Cacheable[In]
                                     ): Unit

  /**
   * - Must lock the workflow while running
   */
  @throws[WorkflowError.InputConflict]
  def runWorkflowInstance[In, Out](
                                    workflowInstance: WorkflowInstanceBuilder[In, Out],
                                    in: In
                                  )(
                                    using Cacheable[In]
                                  ): Out

  /**
   * - Must lock the workflow while running
   * - Must throw a WorkflowError.NotFound
   */
  @throws[WorkflowError.NotFound]
  def recoverWorkflowInstance[In, Out](
                                        workflowInstance: WorkflowInstanceBuilder[In, Out]
                                      )(
                                        using Cacheable[In]
                                      ): Out

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
  trait GenerateIds extends WorkflowRuntime {

    override def generateWorkflowInstanceId: WorkflowInstanceId = WorkflowInstanceId.unsafeMake(UUID.randomUUID().toString)

    override def generateStepIdempotencyId: StepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)

  }
}
