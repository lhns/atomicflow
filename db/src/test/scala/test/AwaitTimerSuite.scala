package test

import atomicflow.*
import atomicflow.given
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*

import java.time.Instant
import scala.concurrent.duration.*

class AwaitTimerSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

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

  private def nonSignalEventCount(workflowId: WorkflowId): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_events WHERE workflow_id = $workflowId AND event_kind <> 'Signal'
      """.query[Int].unique
    )

  test("a timer not yet due suspends and records an absolute-deadline subscription with no event") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "timer-notdue") { in =>
      Step.await[Unit]("t", Awaitable.Timer(5.minutes))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val subs = timerSubscriptions(wf.id, "k")
    assertEquals(subs.length, 1)
    assertEquals(subs.head, Instant.parse("2026-01-02T00:05:00Z"))
    assertEquals(nonSignalEventCount(wf.id), 0)
  }

  test("advancing past the deadline then re-running resolves the timer inline and completes") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "timer-resolve") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerSubscriptions(wf.id, "k").length, 1)

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(nonSignalEventCount(wf.id), 0, "resolution appends no event")
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "the subscription is retired on resolution")

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "a terminal re-run registers nothing")
  }

  test("the deadline is stored once and never recomputed across re-runs") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "timer-deadline") { in =>
      Step.await[Unit]("t", Awaitable.Timer(5.minutes))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val d1 = timerSubscriptions(wf.id, "k").head
    assertEquals(d1, Instant.parse("2026-01-02T00:05:00Z"))

    clock.advanceBy(1.minute)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val d2 = timerSubscriptions(wf.id, "k").head
    assertEquals(d2, d1, "the deadline must not be recomputed on replay")

    clock.advanceBy(6.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
  }

  test("a resolved timer stays resolved across repeated runs of a suspended flow") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "timer-once") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      TestControlFlow.suspend()
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty)

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty,
                 "re-running a suspended-then-resolved flow must not register the timer again")
    assertEquals(nonSignalEventCount(wf.id), 0)
  }

  test("the subscription persists while suspended and is deleted only when the await resolves") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "timer-lifecycle") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute))
      TestControlFlow.suspend()
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerSubscriptions(wf.id, "k").length, 1, "the subscription exists while suspended")

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "the subscription is deleted when the await resolves")
    assertEquals(nonSignalEventCount(wf.id), 0, "resolution leaves no event behind")
  }

  test("manual mode resolves a timer purely via run evaluation, with no sweep or runner") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "timer-manual") { in =>
      Step.await[Unit]("t", Awaitable.Timer(10.seconds))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    clock.advanceBy(20.seconds)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("done"))
  }

  test("invalidateOn re-registers a fresh subscription; the old deadline is not reused") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    var ctx = "A"
    val wf = Workflow[String, String](id = "timer-invalidate") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute), invalidateOn = Seq("ctx" -> ctx))
      TestControlFlow.suspend()
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val d1 = timerSubscriptions(wf.id, "k").head

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "the resolved await retired sub1")

    ctx = "B"
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val subs = timerSubscriptions(wf.id, "k")
    assertEquals(subs.length, 1)
    val d2 = subs.head
    assertNotEquals(d2, d1, "invalidation registers a new deadline")
    assertEquals(d2, Instant.parse("2026-01-02T00:03:00Z"), "the deadline recomputes to now + delay")
  }

  test("invalidateAfter expiry re-registers a fresh subscription with a recomputed deadline; the old deadline is not reused") {
    val clock = new TestClock(Instant.parse("2026-01-02T00:00:00Z"))
    val rt = newRuntime(clock)
    val wf = Workflow[String, String](id = "timer-ttl") { in =>
      Step.await[Unit]("t", Awaitable.Timer(1.minute), invalidateAfter = 10.seconds)
      TestControlFlow.suspend()
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id

    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val d1 = timerSubscriptions(wf.id, "k").head

    clock.advanceBy(2.minutes)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    assertEquals(timerSubscriptions(wf.id, "k"), Vector.empty, "resolution retired sub1")

    clock.advanceBy(20.seconds)
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    val subs = timerSubscriptions(wf.id, "k")
    assertEquals(subs.length, 1)
    val d2 = subs.head
    assertNotEquals(d2, d1, "cache expiry registers a new deadline")
    assertEquals(d2, Instant.parse("2026-01-02T00:03:20Z"), "the deadline recomputes to now + delay")
  }
}
