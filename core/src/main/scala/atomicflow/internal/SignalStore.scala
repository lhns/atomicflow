package atomicflow.internal

import atomicflow.{Signal, WorkflowError}

import scala.concurrent.duration.FiniteDuration

trait SignalStore {
  def getSignalValue[A](signal: Signal[A]): Option[A]

  @throws[WorkflowError.SignalConflict]
  def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration): Unit
}
