package atomicflow.internal

import atomicflow.{Signal, WorkflowError}

import scala.concurrent.duration.FiniteDuration

trait SignalStore {
  def bind(workflowScope: WorkflowScope): SignalStore.Bound
}

object SignalStore {
  trait Bound {
    def getSignalValue[A](signal: Signal[A]): Option[A]

    @throws[WorkflowError.SignalConflict]
    def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration): Unit
  }
}
