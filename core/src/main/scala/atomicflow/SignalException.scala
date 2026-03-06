package atomicflow

trait SignalException(signal: Signal[?])
  extends WorkflowException {
  self: Exception =>

  def signalMeta: SignalMeta = signal.meta
}

object SignalException {
  def signalRef(signal: Signal[?], workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"${WorkflowException.workflowRef(workflowMeta, workflowInstanceId)}/$signal"

  def emptyMessage(signal: Signal[?], workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"Empty signal value: ${signalRef(signal, workflowMeta, workflowInstanceId)}"

  def conflictMessage(signal: Signal[?], workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): String =
    s"Cannot change signal value: ${signalRef(signal, workflowMeta, workflowInstanceId)}"
}

class SignalEmptyException(
                            signal: Signal[?],
                            override val workflowMeta: WorkflowMeta,
                            override val workflowInstanceId: WorkflowInstanceId,
                            message: String
                          )
  extends Exception(message) with SignalException(signal) {
  def this(signal: Signal[?], workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId) =
    this(
      signal,
      workflowMeta,
      workflowInstanceId,
      SignalException.emptyMessage(signal, workflowMeta, workflowInstanceId)
    )
}

class SignalConflictException(
                               signal: Signal[?],
                               override val workflowMeta: WorkflowMeta,
                               override val workflowInstanceId: WorkflowInstanceId,
                               message: String
                             )
  extends Exception(message) with SignalException(signal) {
  def this(signal: Signal[?], workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId) =
    this(
      signal,
      workflowMeta,
      workflowInstanceId,
      SignalException.conflictMessage(signal, workflowMeta, workflowInstanceId)
    )
}
