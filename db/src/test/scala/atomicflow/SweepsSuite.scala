package atomicflow

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import java.time.Instant
import scala.concurrent.duration.*

/** Background sweeps: timer firing (Path 1), cancellation escalation, and lease
  * recovery. Lives in package `atomicflow` so it can invoke the runner's
  * package-private sweep hooks (`runTimerSweep` etc.) and the loop-less
  * `startJobRunnerForTests` deterministically.
  */
class SweepsSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def waitUntil(cond: => Boolean, timeout: FiniteDuration = 10.seconds): Unit = {
    val deadline = System.nanoTime() + timeout.toNanos
    while (!cond && System.nanoTime() < deadline) Thread.sleep(10)
    assert(cond, "condition not met within timeout")
  }

  private def wakeupExists(workflowId: WorkflowId, key: WorkflowInstanceKey): Boolean =
    run(
      sql"""
        SELECT 1 FROM workflow_wakeups WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Int].option
    ).isDefined

  private def terminalRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[String], Option[String])] =
    run(
      sql"""
        SELECT terminal_state, terminal_outcome FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[
          (Option[String], Option[String])
        ].option
    )

  private def leaseRow(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Option[(Option[Instant], Long)] =
    run(
      sql"""
        SELECT lease_expires_at, fencing_token FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[
          (Option[Instant], Long)
        ].option
    )

  private def cancelRequestedAt(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[Instant] =
    run(
      sql"""
        SELECT cancel_requested_at FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Option[Instant]].unique
    )

  private def completedOutcome(workflowId: WorkflowId, key: WorkflowInstanceKey): Option[String] =
    run(
      sql"""
        SELECT terminal_outcome FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}' AND terminal_state IS NOT NULL
      """.query[Option[String]].option
    ).flatten

  private def nonSignalEventCount(workflowId: WorkflowId): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $workflowId AND event_kind <> 'Signal'
      """.query[Int].unique
    )

  private def timerSubscriptions(
      workflowId: WorkflowId,
      key: WorkflowInstanceKey
  ): Vector[Instant] =
    run(
      sql"""
        SELECT deadline FROM workflow_subscriptions
        WHERE kind = 'timer' AND workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = '{}'
      """.query[Instant].to[Vector]
    )

  test("timer sweep wakes an unattended instance with a due timer; the run resolves the timer") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "ts-fire") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      s"done-$in"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      assertEquals(nonSignalEventCount(wf.id), 0)

      clock.advanceBy(2.minutes)
      assertEquals(runner.runTimerSweep(), 1)

      assertEquals(timerSubscriptions(wf.id, "k").length, 1, "the sweep itself resolves nothing")
      assertEquals(nonSignalEventCount(wf.id), 0, "the sweep appends no event")
      assert(wakeupExists(wf.id, "k"), "the sweep must upsert the owning instance's wakeup")

      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done-in"))
      assertEquals(timerSubscriptions(wf.id, "k").length, 0, "the resolved timer's subscription is deleted")
    } finally runner.stop(1.second)
  }

  test("timer sweep leaves NOT-due subscriptions untouched") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "ts-notdue") { in =>
      Step.await[Unit]("t", Awaitable.Timer(5.minutes))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

      assertEquals(runner.runTimerSweep(), 0, "no timer is due, so nothing may be woken")
      assert(!wakeupExists(wf.id, "k"))
      assertEquals(timerSubscriptions(wf.id, "k").length, 1, "the subscription must survive")
    } finally runner.stop(1.second)
  }

  test("timer sweep is idempotent across passes: one event total") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "ts-idem") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

      clock.advanceBy(2.minutes)
      assertEquals(runner.runTimerSweep(), 1)
      assertEquals(runner.runTimerSweep(), 0, "a second pass finds the wakeup already present")
      assertEquals(nonSignalEventCount(wf.id), 0)
    } finally runner.stop(1.second)
  }

  test("timer sweep wakes every due instance in a batch") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "ts-order") { in =>
      Step.await[Unit]("t", Awaitable.Timer(in.toInt.minutes))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val ids = Vector("a" -> "1", "b" -> "2", "c" -> "3").map { case (k, delay) =>
        val id = rt.createWorkflowInstance(wf, k, delay).id
        assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
        (k, id)
      }
      clock.advanceBy(5.minutes)
      assertEquals(runner.runTimerSweep(), 3)

      ids.foreach { case (k, _) => assert(wakeupExists(wf.id, k), s"instance $k must be woken") }
    } finally runner.stop(1.second)
  }

  test("escalation sweep turns an overdue cancellation into TERMINATED") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "escalate") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      rt.cancel(id)
      assert(cancelRequestedAt(wf.id, "k").isDefined)

      clock.advanceBy(1.minute)
      assertEquals(runner.runEscalationSweep(), 1)

      assertEquals(terminalRow(wf.id, "k").get._1, Some("terminated"))
      val outcome = completedOutcome(wf.id, "k").get
      assertEquals(
        Cacheable[WorkflowCompletionResult[String]].read(outcome),
        WorkflowCompletionResult.Terminated
      )
    } finally runner.stop(1.second)
  }

  test("escalation sweep ignores cancellations that have not yet exceeded cancelTimeout") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "escalate-pending") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      rt.cancel(id)

      clock.advanceBy(100.millis)
      assertEquals(runner.runEscalationSweep(), 0)
      assertEquals(terminalRow(wf.id, "k").get._1, None, "the instance must stay CANCELLING")
      assertEquals(completedOutcome(wf.id, "k"), None)
    } finally runner.stop(1.second)
  }

  test("escalation sweep ignores terminal instances and is idempotent") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "escalate-terminal") { in => s"out-$in" }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("out-in"))
      run(
        sql"""
          UPDATE workflow_instances SET cancel_requested_at = '2020-01-01T00:00:00Z'
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        """.update.run
      )

      assertEquals(runner.runEscalationSweep(), 0)
      assertEquals(terminalRow(wf.id, "k").get._1, Some("completed"), "a terminal instance must be untouched")

      clock.advanceBy(1.minute)
      assertEquals(runner.runEscalationSweep(), 0)
      assertEquals(completedOutcome(wf.id, "k"), Some(Cacheable[WorkflowCompletionResult[String]].write(WorkflowCompletionResult.Completed("out-in"))))
    } finally runner.stop(1.second)
  }

  test("escalation sweep is idempotent: running it again does not re-terminate") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "escalate-idem") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      rt.cancel(id)
      clock.advanceBy(1.minute)

      assertEquals(runner.runEscalationSweep(), 1)
      assertEquals(runner.runEscalationSweep(), 0)
      assertEquals(terminalRow(wf.id, "k").get._1, Some("terminated"))
      assertEquals(
        completedOutcome(wf.id, "k").map(Cacheable[WorkflowCompletionResult[String]].read),
        Some(WorkflowCompletionResult.Terminated),
        "the terminal outcome is set once"
      )
      assertEquals(nonSignalEventCount(wf.id), 0)
    } finally runner.stop(1.second)
  }

  test("recovery sweep wakes an instance with an expired lease; the claim takes it over and advances the token") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "recover") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      val before = leaseRow(wf.id, "k").get._2

      clock.advanceBy(1.hour)
      run(
        sql"""
          UPDATE workflow_instances
          SET lease_expires_at = '2026-01-02T00:30:00Z'
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        """.update.run
      )

      assertEquals(runner.runRecoverySweep(), 1)
      assert(wakeupExists(wf.id, "k"), "recovery must upsert the instance's wakeup")
      assertEquals(leaseRow(wf.id, "k").get._2, before, "recovery itself is wake-only")
      assertEquals(runner.runRecoverySweep(), 0, "the wakeup marks the instance as handled; it is not re-selected")

      runner.runDriverCycle()
      assertEquals(runner.lastClaimedBatch, Vector(id), "the claim takes over the expired lease")
      assert(leaseRow(wf.id, "k").get._2 > before, "the takeover advances the fencing token to fence the crashed run")
    } finally runner.stop(1.second)
  }

  test("recovery fences a stale in-flight run: its heartbeat throws LeaseLostException and the instance is re-scheduled") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val bodyStarted = new java.util.concurrent.CountDownLatch(1)
    val releaseBody = new java.util.concurrent.CountDownLatch(1)
    val captured = new java.util.concurrent.atomic.AtomicReference[Option[Throwable]](None)
    val done = new java.util.concurrent.CountDownLatch(1)
    val wf = Workflow[String, String](id = "recover-fence") { in =>
      Step.atLeastOnce[String]("step") {
        bodyStarted.countDown()
        releaseBody.await(10, java.util.concurrent.TimeUnit.SECONDS)
        Workflow.heartbeat()
        s"out-$in"
      }
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    val thread = new Thread(() => {
      try {
        rt.createAndRun(wf, "k", "in")
        captured.set(None)
      } catch { case t: Throwable => captured.set(Some(t)) } finally done.countDown()
    })
    try {
      thread.start()
      waitUntil(bodyStarted.getCount == 0)

      // The run's lease expires while the body is paused; recovery wakes the
      // instance and a new owner takes the expired lease over.
      clock.advanceBy(1.hour)
      assertEquals(runner.runRecoverySweep(), 1)
      assert(wakeupExists(wf.id, "k"), "recovery must upsert the instance's wakeup")
      assert(
        rt.runTransaction(rt.tryAcquireLeaseQ(wf.id, "k", ScopePath.empty)).isDefined,
        "a new owner takes over the expired lease"
      )

      // The stale run resumes: its next fenced write (the heartbeat) must be
      // fenced out by the takeover's token advance.
      releaseBody.countDown()
      waitUntil(done.getCount == 0)
      assert(captured.get().exists(_.isInstanceOf[LeaseLostException]),
        s"the stale run must lose its lease, got ${captured.get()}")

      // The aborted run left no durable effect: not terminal, no cached step
      // result, and the recovery's wakeup still re-schedules the instance.
      val row = run(
        sql"""
          SELECT terminal_state FROM workflow_instances
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        """.query[Option[String]].unique
      )
      assertEquals(row, None, "the fenced run must not terminalize the instance")
      val stepState = run(
        sql"""
          SELECT state_kind FROM workflow_steps
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}' AND step_id = 'step'
        """.query[String].option
      )
      assertEquals(stepState, Some("Started"), "the fenced run must not cache a step result")
      assert(wakeupExists(wf.id, "k"), "the recovery's wakeup must survive the fenced run")
    } finally {
      releaseBody.countDown()
      thread.join(10.seconds.toMillis)
      runner.stop(1.second)
    }
  }

  test("recovery sweep ignores live leases and terminal instances") {
    val rt = newRuntime
    val sig = Signal[String]("approve")
    val wf = Workflow[String, String](id = "recover-live") { in =>
      Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      run(
        sql"""
          UPDATE workflow_instances
          SET lease_expires_at = now() + interval '1 hour'
          WHERE workflow_id = ${wf.id} AND workflow_instance_key = 'k' AND workflow_instance_scope = '{}'
        """.update.run
      )

      assertEquals(runner.runRecoverySweep(), 0)
      val row = leaseRow(wf.id, "k").get
      assert(row._1.exists(_.isAfter(Instant.now())), "a live lease must be untouched")

      val wf2 = Workflow[String, String](id = "recover-terminal") { in => s"out-$in" }
      val id2 = rt.createWorkflowInstance(wf2, "t", "in").id
      assertEquals(rt.runWorkflowInstance(wf2, id2), WorkflowRunResult.Result("out-in"))
      run(
        sql"""
          UPDATE workflow_instances
          SET lease_expires_at = now() - interval '1 hour'
          WHERE workflow_id = ${wf2.id} AND workflow_instance_key = 't' AND workflow_instance_scope = '{}'
        """.update.run
      )
      assertEquals(runner.runRecoverySweep(), 0, "a terminal instance's stale lease is harmless and left alone")
      assert(!wakeupExists(wf2.id, "t"), "the terminal instance is never re-scheduled")
    } finally runner.stop(1.second)
  }

  test("a due timer whose site is not reached on the next run does not wake the instance again") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val sig = Signal[String]("go")
    val awaitTimer = new java.util.concurrent.atomic.AtomicBoolean(true)
    val wf = Workflow[String, String](id = "orphan-timer") { in =>
      if (awaitTimer.get()) Step.await[Unit]("t", Awaitable.Timer(1.minute))
      else Step.await[String]("w", Awaitable.SignalEvent(sig))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(wf, "k", "in").id
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
      // The next run takes another path: the timer site is never evaluated again.
      awaitTimer.set(false)
      clock.advanceBy(2.minutes)

      assertEquals(runner.runTimerSweep(), 1)
      runner.runDriverCycle()
      assertEquals(runner.lastClaimedBatch, Vector(id))
      assert(!wakeupExists(wf.id, "k"), "the run consumed the wakeup and suspended on the signal")

      assertEquals(runner.runTimerSweep(), 0, "the run started after the deadline; the orphaned timer is not selected again")
      runner.runDriverCycle()
      assertEquals(runner.lastClaimedBatch, Vector.empty, "nothing wakes the instance again")
      assert(!wakeupExists(wf.id, "k"))
    } finally runner.stop(1.second)
  }

  test("a run that keeps writing steps past leaseDuration keeps its lease") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock, leaseDuration = 10.seconds)
    val takeover = new java.util.concurrent.atomic.AtomicReference[Option[Long]](None)
    val woken = new java.util.concurrent.atomic.AtomicBoolean(true)
    val wf = Workflow[String, String](id = "lease-progress") { in =>
      Step.atLeastOnce[String]("a") { clock.advanceBy(6.seconds); "a" }
      Step.atLeastOnce[String]("b") { clock.advanceBy(6.seconds); "b" }
      Step.atLeastOnce[String]("c") {
        // 12s after acquisition: past the original 10s lease, but every step
        // write renewed it.
        takeover.set(rt.runTransaction(rt.tryAcquireLeaseQ("lease-progress", "k", ScopePath.empty)))
        woken.set(rt.recoverLease(WorkflowInstanceId("lease-progress", "k")))
        "c"
      }
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(takeover.get(), None, "the lease must still be live, so no one can take it over")
    assertEquals(woken.get(), false, "recovery must not treat the progressing run as crashed")
  }

  test("cadence wiring: an unattended suspended-on-timer instance completes autonomously") {
    val rt = newRuntime
    val wf = Workflow[String, String](id = "cadence") { in =>
      Step.await[Unit]("t", Awaitable.Timer(100.millis))
      "done"
    }
    val runner = rt.startJobRunner(Seq(wf), JobRunnerSettings.forTests)
    try {
      val inst = wf.createAndScheduleForAsyncRun("k", "in")(using rt)
      assertEquals(inst.blockUntilCompleted(10.seconds)(using rt), WorkflowRunResult.Result("done"))
    } finally runner.stop(1.second)
  }

  test("sweeps are definition-agnostic: they service a workflow outside the runner's registry") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val registered = Workflow[String, String](id = "registered") { in => s"out-$in" }
    val unknown = Workflow[String, String](id = "unknown") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      "done"
    }
    val runner = rt.startJobRunnerForTests(Seq(registered), JobRunnerSettings.forTests)
    try {
      val id = rt.createWorkflowInstance(unknown, "u", "in").id
      assertEquals(rt.runWorkflowInstance(unknown, id), WorkflowRunResult.WorkflowSuspended)

      clock.advanceBy(2.minutes)
      assertEquals(runner.runTimerSweep(), 1, "the sweep wakes the unknown workflow's instance")
      assert(wakeupExists(unknown.id, "u"), "the unknown workflow's wakeup is upserted")

      runner.runDriverCycle()
      assert(wakeupExists(unknown.id, "u"), "the wakeup ages unclaimed: not in this runner's registry")
    } finally runner.stop(1.second)
  }

  test("a runner-driven failure uses the settings throwableCacheable to encode its terminal outcome") {
    val rt = newRuntime
    val custom = new Cacheable[Throwable] {
      override def stableSerializedTypeId: String = "test-throwable-marker"
      override def write(value: Throwable): String = "MARKER:" + value.getClass.getName
      override def read(serialized: String): Throwable = new RuntimeException(serialized)
    }
    val wf = Workflow[String, String](id = "codec") { _ => throw new RuntimeException("boom") }
    rt.createAndScheduleForAsyncRun(wf, "k", "x")
    val runner = rt.startJobRunnerForTests(
      Seq(wf),
      JobRunnerSettings.forTests.copy(throwableCacheable = custom)
    )
    try {
      runner.runDriverCycle()
      val outcome = terminalRow(wf.id, "k").get._2.get
      assert(outcome.contains("test-throwable-marker"), s"outcome must embed the custom codec id: $outcome")
      assert(outcome.contains("MARKER:java.lang.RuntimeException"), s"outcome must use the custom codec: $outcome")
    } finally runner.stop(1.second)
  }
}
