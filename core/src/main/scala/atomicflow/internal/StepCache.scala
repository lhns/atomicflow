package atomicflow.internal

import atomicflow.internal.StepInputFingerprints
import atomicflow.{Cacheable, StepIdempotencyId, WorkflowError}

import scala.concurrent.duration.FiniteDuration

trait StepCache {
  def bind[Out: Cacheable](stepScope: StepScope): StepCache.Bound[Out]
}

object StepCache {
  trait Bound[Out] {
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
}
