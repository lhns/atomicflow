package atomicflow

import atomicflow.Cacheable.Simple.given
import test.PostgresWorkflowRuntimeSuite

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*

/** Lock-order regressions: transactions touching the same instance must not
  * deadlock. Interleavings are forced by holding the global event-append
  * advisory lock on a raw connection and waiting (by polling
  * `pg_stat_activity`, not by sleeping) until the competing transactions block.
  */
class LockOrderSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  /** Backends of this database currently waiting on a heavyweight lock. */
  private def blockedBackends(): Int = {
    val c = rawConnection
    try {
      val rs = c.createStatement().executeQuery(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'"
      )
      rs.next()
      rs.getInt(1)
    } finally c.close()
  }

  /** Polls until `cond`; the deadline only bounds a hang, it orders nothing. */
  private def awaitCondition(what: String)(cond: => Boolean): Unit = {
    val deadline = System.nanoTime() + 60.seconds.toNanos
    while (!cond) {
      if (System.nanoTime() > deadline) fail(s"timed out waiting for: $what")
      Thread.onSpinWait()
    }
  }

  private final class Worker(name: String)(body: => Unit) {
    val failure = new AtomicReference[Option[Throwable]](None)
    private val thread = new Thread(() =>
      try body
      catch { case t: Throwable => failure.set(Some(t)) },
      name
    )
    thread.start()
    def done: Boolean = !thread.isAlive
    def join(): Unit = thread.join()
  }

  test("timer sweep and sendSignal on the same instance do not deadlock") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val sig = Signal[String]("go")
    val wf = Workflow[String, String](id = "lock-timer-signal") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      s"done-$in"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    clock.advanceBy(2.minutes)
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)

    // Gate: hold the append lock so both transactions queue behind it.
    val gate = rawConnection
    gate.setAutoCommit(false)
    gate.createStatement().execute(s"SELECT pg_advisory_xact_lock(${rt.EventAppendLockKey})")
    try {
      val sweep = new Worker("sweep")(runner.runTimerSweep())
      awaitCondition("the sweep blocks or finishes")(sweep.done || blockedBackends() == 1)
      val sweepBlocked = if (sweep.done) 0 else 1
      val send = new Worker("send")(rt.sendSignal(id, sig.key, "x"))
      awaitCondition("the send blocks behind the sweep or the gate")(send.done || blockedBackends() == sweepBlocked + 1)
      gate.rollback()
      sweep.join()
      send.join()
      assertEquals(sweep.failure.get(), None)
      assertEquals(send.failure.get(), None)
    } finally {
      gate.close()
      runner.stop(1.second)
    }
  }
}
