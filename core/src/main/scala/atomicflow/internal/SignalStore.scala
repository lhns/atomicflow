package atomicflow.internal

import atomicflow.{Signal, SignalConflictException}

import scala.concurrent.duration.FiniteDuration

trait SignalStore {
  def bind(workflowScope: WorkflowScope): SignalStore.Bound
}

object SignalStore {
  trait Bound {
    //@throws[SignalEmptyException]
    def getSignalValue[A](signal: Signal[A]): Option[A]

    @throws[SignalConflictException]
    def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration): Unit
  }
}
