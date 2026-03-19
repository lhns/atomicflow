package atomicflow.internal

import atomicflow.StepIdempotencyId

trait StepIdempotencyStore {
  /**
   * Get or create a stepIdempotencyId
   * The stepIdempotencyId should be namespaced by the tuple
   * (libraryVersion, workflowId, workflowInstanceId, stepId, stepVersion, inputFingerprints)
   */
  def acquireStepIdempotencyId(inputFingerprints: StepInputFingerprints): StepIdempotencyId

  /**
   * Get or create a stepIdempotencyId
   * The stepIdempotencyId should be namespaced by the tuple
   * (libraryVersion, workflowId, workflowInstanceId, stepId)
   */
  def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId
}
