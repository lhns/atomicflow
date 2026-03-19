package atomicflow.internal

import atomicflow.{Cacheable, StepIdempotencyId, WorkflowError}

import scala.concurrent.duration.FiniteDuration

trait StepCache[Out] {
  @throws[WorkflowError.StepConflict]
  def get(
           stepIdempotencyId: StepIdempotencyId,
           inputFingerprints: StepInputFingerprints
         ): Option[Out]

  @throws[WorkflowError.StepConflict]
  def put(
           stepIdempotencyId: StepIdempotencyId,
           inputFingerprints: StepInputFingerprints,
           value: Out,
           ttl: FiniteDuration
         ): Unit
}
