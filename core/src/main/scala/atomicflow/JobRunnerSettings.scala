package atomicflow

import java.util.concurrent.Executor
import scala.concurrent.duration.*

/** Tunables for [[WorkflowRuntime.startJobRunner]]. The runner owns a daemon
  * worker pool sized by [[workerThreads]] unless [[executor]] overrides it with
  * a user-supplied [[java.util.concurrent.Executor]].
  *
  * @param workerThreads
  *   runtime-owned daemon worker pool size; ignored when `executor` is set
  * @param executor
  *   user-supplied pool override; when set, the runner never shuts it down
  * @param pollInterval
  *   wakeup poll cadence; jittered per process
  * @param wakeupBatchSize
  *   max wakeups claimed per cycle
  * @param timerSweepInterval
  *   cadence of the timer wake sweep (wakes instances with a due, unhandled timer)
  * @param timerBatchSize
  *   max instances the timer sweep wakes per pass
  * @param sweepInterval
  *   cadence of the escalation and lease-recovery sweeps
  * @param fullSubscriptionScanInterval
  *   cadence of the full subscription scan that guarantees no lost wakeups
  * @param leaseAcquireTimeout
  *   bound an external run waits for a leased instance
  * @param cancelTimeout
  *   how long a cancellation may run before escalation to TERMINATED
 * @param throwableCacheable
 *   codec used to encode runner-driven terminal failure outcomes
 */
final case class JobRunnerSettings(
    workerThreads: Int = 8,
    executor: Option[Executor] = None,
    pollInterval: FiniteDuration = 250.millis,
    wakeupBatchSize: Int = 32,
    timerSweepInterval: FiniteDuration = 250.millis,
    timerBatchSize: Int = 128,
    sweepInterval: FiniteDuration = 1.second,
    fullSubscriptionScanInterval: FiniteDuration = 1.minute,
    leaseAcquireTimeout: FiniteDuration = 30.seconds,
    cancelTimeout: FiniteDuration = 5.minutes,
    throwableCacheable: Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer
)

object JobRunnerSettings {

  /** Production defaults: modest worker count, a few hundred millisecond poll. */
  def default: JobRunnerSettings = JobRunnerSettings()

  /** Test-friendly defaults: one worker for deterministic ordering, tiny
    * intervals for prompt background progress, short acquire/cancel timeouts so
    * escalation tests run quickly. The lease duration is the runtime's.
    */
  def forTests: JobRunnerSettings =
    JobRunnerSettings(
      workerThreads = 1,
      pollInterval = 25.millis,
      timerSweepInterval = 25.millis,
      sweepInterval = 50.millis,
      fullSubscriptionScanInterval = 25.millis,
      leaseAcquireTimeout = 1.second,
      cancelTimeout = 500.millis
    )
}
