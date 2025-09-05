package atomicflow

trait StepException(using stepCtx: StepContext[?])
  extends WorkflowException {
  self: Exception =>

  def stepMeta: StepMeta = stepCtx.meta
}

class StepInputConflictException(message: String)
                                (using stepCtx: StepContext[?])
  extends Exception(message) with StepException {

  def this()(using stepCtx: StepContext[?]) =
    this(s"Cannot re-run once-step with different input: $stepCtx)")
}

class StepUnknownStateException(message: String)
                               (using stepCtx: StepContext[?])
  extends Exception(message) with StepException {

  def this()(using stepCtx: StepContext[?]) =
    this(s"Cannot re-run step with unknown state. This could be caused by an exception in a once-step: $stepCtx)")
}
