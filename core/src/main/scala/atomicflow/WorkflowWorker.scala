package atomicflow

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

/** Runs root instances of the registered workflows whenever they are woken up: on creation, when a signal is set
  * anywhere in their tree, and for retries of failed or interrupted runs (with exponential backoff).
  *
  * Child workflows need no registration: they run inline in their root's pass. Several workers (also in different
  * services with different registrations) can share one runtime; each only claims wakeups of its own workflows.
  *
  * Evaluation stays self-sufficient: the worker only drives `recover()`, so everything it does can also be done
  * manually (e.g. in tests).
  *
  * @param onError called for runs that failed; they are already rescheduled with backoff by the runtime. */
class WorkflowWorker(
                      workflows: Seq[Workflow[?, ?]],
                      lockedRetryDelay: FiniteDuration = 1.second,
                      onError: (WorkflowRuntime.Wakeup, Throwable) => Unit = (_, _) => ()
                    )(using rt: WorkflowRuntime) {
  private val registry: Map[WorkflowId, Workflow[?, ?]] = workflows.map(workflow => workflow.meta.id -> workflow).toMap

  /** Runs all due root instances (up to `limit`) once. Returns how many were claimed. */
  def runOnce(limit: Int = 16): Int = {
    val wakeups = rt.claimWakeups(registry.keySet, limit)
    wakeups.foreach { wakeup =>
      try registry(wakeup.workflowId).recover(wakeup.instanceId)
      catch {
        case _: WorkflowError.SignalEmpty => // pending: woken up again when a signal is set
        case _: WorkflowError.Locked => rt.scheduleWakeup(wakeup.workflowId, wakeup.instanceId, lockedRetryDelay)
        case NonFatal(e) => onError(wakeup, e)
      }
    }
    wakeups.size
  }

  /** Starts a daemon thread that keeps running due instances, polling every `pollInterval` when idle. */
  def start(pollInterval: FiniteDuration = 1.second): AutoCloseable = {
    @volatile var running = true
    val thread = new Thread(() => {
      while (running) {
        val claimed =
          try runOnce()
          catch case NonFatal(_) => 0
        if (claimed == 0 && running)
          try Thread.sleep(pollInterval.toMillis)
          catch case _: InterruptedException => ()
      }
    }, "atomicflow-worker")
    thread.setDaemon(true)
    thread.start()
    () => {
      running = false
      thread.interrupt()
      thread.join()
    }
  }
}
