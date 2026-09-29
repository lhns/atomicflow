package atomicflow.internal

import atomicflow.{StepIdempotencyId, WorkflowError}

import scala.concurrent.duration.FiniteDuration

/** The durable state of a step execution. */
enum StepState[+Out] {
  case NotStarted

  /** The step's body was started but did not record an outcome (only written for at-most-once steps). */
  case Started

  case Completed(value: Out)
}

trait StepCache[Out] {
  @throws[WorkflowError.StepConflict]
  def get(
           stepIdempotencyId: StepIdempotencyId,
           inputFingerprints: StepInputFingerprints
         ): StepState[Out]

  /** Durably records that the step's body is about to run. */
  @throws[WorkflowError.Locked]
  def markStarted(
                   stepIdempotencyId: StepIdempotencyId,
                   inputFingerprints: StepInputFingerprints
                 ): Unit

  /** Removes a `Started` marker again, e.g. after a retryable failure. Never removes a completed result. */
  def clearStarted(stepIdempotencyId: StepIdempotencyId): Unit

  @throws[WorkflowError.StepConflict]
  @throws[WorkflowError.Locked]
  def put(
           stepIdempotencyId: StepIdempotencyId,
           inputFingerprints: StepInputFingerprints,
           value: Out,
           ttl: FiniteDuration
         ): Unit
}
