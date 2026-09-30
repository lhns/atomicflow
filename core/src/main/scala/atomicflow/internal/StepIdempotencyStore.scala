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
   * (libraryVersion, workflowId, workflowInstanceId, stepId).
   * If the recorded `keyFingerprints` (the step's invalidating inputs) differ, the current id is replaced by a new
   * one, so the step runs again.
   */
  def acquireOnlyOnceStepIdempotencyId(keyFingerprints: StepInputFingerprints): StepIdempotencyId
}
