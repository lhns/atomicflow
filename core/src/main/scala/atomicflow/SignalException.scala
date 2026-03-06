package atomicflow

trait SignalException(signal: Signal[?])
  extends WorkflowException {
  self: Exception =>

  def signalMeta: SignalMeta = signal.meta
}

class SignalEmptyException(
                            signal: Signal[?],
                            message: String,
                            override val workflowMeta: WorkflowMeta,
                            override val workflowInstanceId: WorkflowInstanceId
                          )
  extends Exception(message) with SignalException(signal) {

  def this(signal: Signal[?])(using workflowCtx: SimpleWorkflowContext) =
    this(
      signal,
      s"Empty signal value: $workflowCtx/$signal",
      workflowCtx.meta,
      workflowCtx.instanceId
    )
}

class SignalConflictException(
                               signal: Signal[?],
                               message: String,
                               override val workflowMeta: WorkflowMeta,
                               override val workflowInstanceId: WorkflowInstanceId
                             )
  extends Exception(message) with SignalException(signal) {

  def this(signal: Signal[?])(using workflowCtx: SimpleWorkflowContext) =
    this(
      signal,
      s"Cannot change signal value: $workflowCtx/$signal)",
      workflowCtx.meta,
      workflowCtx.instanceId
    )
}
