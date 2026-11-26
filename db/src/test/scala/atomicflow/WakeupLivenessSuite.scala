package atomicflow

import atomicflow.*
import atomicflow.Cacheable.Simple.given
import doobie.implicits.*
import doobie.postgres.implicits.*
import test.PostgresWorkflowRuntimeSuite

import scala.concurrent.duration.*

/** Wakeup liveness: writers wake the subscribers they affect (a signal send wakes its subscribed addressee in its
  * own transaction and the inheriting descendants right after the commit; a terminal transition wakes the
  * completion subscribers right after the commit). A periodic full subscription scan is the correctness guarantee
  * for a wake lost between a commit and its after-commit wake, or for a subscription committing concurrently with
  * a cross-instance fact. Lives in package `atomicflow` so it can drive the package-private `runDriverCycle` hook
  * and the `private[atomicflow]` scan hook deterministically.
  */
class WakeupLivenessSuite extends PostgresWorkflowRuntimeSuite {

  given Cacheable[Throwable] = Cacheable.forThrowable.genericStringMessageSerializer

  private def wakeupExists(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: ScopePath): Boolean =
    run(
      sql"""
        SELECT 1 FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.query[
        Int].option
    ).isDefined

  private def wakeupCount(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: ScopePath): Int =
    run(
      sql"""
        SELECT COUNT(*) FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.query[
        Int].unique
    )

  private def deleteWakeup(workflowId: WorkflowId, key: WorkflowInstanceKey, scope: ScopePath): Unit =
    run(
      sql"""
        DELETE FROM workflow_wakeups
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = ${scope.segments}
      """.update.run
    )

  test("a send wakes a suspended direct-signal subscriber without any runner pass") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "liv-direct") { _ =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val inst = rt.createWorkflowInstance(wf, "k", "in")
    val id = inst.id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(wf.id, "k", ScopePath.empty)

    signal.send(id, "hi")(using rt)
    assert(wakeupExists(wf.id, "k", ScopePath.empty), "the send must wake its subscribed addressee itself")

    val runner = rt.startJobRunnerForTests(Seq(wf), JobRunnerSettings.forTests)
    try {
      runner.runDriverCycle()
      assertEquals(inst.blockUntilCompleted(5.seconds)(using rt), WorkflowRunResult.Result("got:hi"))
    } finally runner.stop(1.second)
  }

  test("a child's completion wakes its completion subscriber; the full scan recovers a lost wakeup") {
    val rt = newRuntime
    val child = Workflow[String, Int]("liv-child") { in => in.length }
    val parent = Workflow[String, String]("liv-parent") { _ =>
      val c = child.startAsChild("c", "hi")
      val r = Step.await[WorkflowCompletionResult[Int]]("await-child", c.completion)
      r match {
        case WorkflowCompletionResult.Completed(v) => s"child:$v"
        case other                                 => s"other:$other"
      }
    }
    val parentId = rt.createWorkflowInstance(parent, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parent, parentId), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(parent.id, "p", ScopePath.empty)
    val childId = rt.getChildWorkflowInstances(parentId).head.id
    assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.Result(2))
    assert(
      wakeupExists(parent.id, "p", ScopePath.empty),
      "the child's terminal commit must wake its completion subscriber"
    )

    // Simulate a crash between the terminal commit and the wake.
    deleteWakeup(parent.id, "p", ScopePath.empty)
    assert(!wakeupExists(parent.id, "p", ScopePath.empty), "the parent's wakeup is lost")

    val settings = JobRunnerSettings.forTests.copy(
      fullSubscriptionScanInterval = 0.millis
    )
    // The runner's registry holds only a bystander workflow, so the claim never
    // touches the parent's wakeup while the full scan (registry-independent)
    // still runs every cycle.
    val bystander = Workflow[String, String](id = "liv-parent-bystander")(in => s"out-$in")
    val runner = rt.startJobRunnerForTests(Seq(bystander), settings)
    try {
      runner.runDriverCycle()
      assert(wakeupExists(parent.id, "p", ScopePath.empty), "the full scan must recreate the lost completion wakeup")
      assertEquals(rt.runWorkflowInstance(parent, parentId), WorkflowRunResult.Result("child:2"))
    } finally runner.stop(1.second)
  }

  test("a send wakes an inheriting descendant; the full scan recovers a lost inherited-signal wakeup") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val child = Workflow[String, String]("liv-worker") { _ =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val parent = Workflow[String, String]("liv-orders") { _ =>
      child.startAsChild("c", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parent, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parent, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = rt.getChildWorkflowInstances(parentId).head.id
    assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(child.id, childId.workflowInstanceKey, childId.scope)
    assert(
      !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "the child's creation wakeup is cleared"
    )

    signal.send(parentId, "x")(using rt)
    assert(
      wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "the send wakes the inheriting descendant right after its commit"
    )

    // Simulate a crash between the send commit and the descendant wake.
    deleteWakeup(child.id, childId.workflowInstanceKey, childId.scope)
    assert(
      !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "the descendant's wakeup is lost"
    )

    val settings = JobRunnerSettings.forTests.copy(
      fullSubscriptionScanInterval = 0.millis
    )
    val bystander = Workflow[String, String](id = "liv-child-bystander")(in => s"out-$in")
    val runner = rt.startJobRunnerForTests(Seq(bystander), settings)
    try {
      runner.runDriverCycle()
      assert(
        wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
        "the full scan must recreate the lost inherited-signal wakeup"
      )
      assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.Result("got:x"))
    } finally runner.stop(1.second)
  }

  test("full scan recovers an inherited-signal await with a some(...) prefix that contains 'some:'") {
    val rt = newRuntime
    val signal = Signal[String]("some:pay-1")
    val child = Workflow[String, String]("liv-worker-some") { _ =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val parent = Workflow[String, String]("liv-orders-some") { _ =>
      child.startAsChild("c", "hi", inheritSignals = SignalInheritance.some(Seq("some:pay")))
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parent, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parent, parentId), WorkflowRunResult.WorkflowSuspended)
    val childId = rt.getChildWorkflowInstances(parentId).head.id
    assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(child.id, childId.workflowInstanceKey, childId.scope)
    assert(
      !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "the child's creation wakeup is cleared"
    )

    signal.send(parentId, "x")(using rt)
    assert(
      wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "the send wakes the inheriting descendant right after its commit"
    )

    // Simulate a crash between the send commit and the descendant wake.
    deleteWakeup(child.id, childId.workflowInstanceKey, childId.scope)
    assert(
      !wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
      "the descendant's wakeup is lost"
    )

    val settings = JobRunnerSettings.forTests.copy(
      fullSubscriptionScanInterval = 0.millis
    )
    val bystander = Workflow[String, String](id = "liv-child-some-bystander")(in => s"out-$in")
    val runner = rt.startJobRunnerForTests(Seq(bystander), settings)
    try {
      runner.runDriverCycle()
      assert(
        wakeupExists(child.id, childId.workflowInstanceKey, childId.scope),
        "the full scan must recreate the lost inherited-signal wakeup for a some(...) prefix containing 'some:'"
      )
      assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.Result("got:x"))
    } finally runner.stop(1.second)
  }

  test("full scan does not wake an unsatisfied subscription or a terminal target") {
    val rt = newRuntime
    val never = Signal[String]("never")
    val wf = Workflow[String, String](id = "liv-never") { _ =>
      Step.await[String]("w", Awaitable.SignalEvent(never))
      "done"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)

    val done = Workflow[String, String](id = "liv-done") { in => s"out-$in" }
    val doneId = rt.createWorkflowInstance(done, "k", "in").id
    assertEquals(rt.runWorkflowInstance(done, doneId), WorkflowRunResult.Result("out-in"))

    val runner = rt.startJobRunnerForTests(
      Seq(wf, done),
      JobRunnerSettings.forTests.copy(fullSubscriptionScanInterval = 0.millis)
    )
    try {
      runner.runDriverCycle()
      assert(!wakeupExists(wf.id, "k", ScopePath.empty), "a subscription with no candidate event must not be woken")
      assert(!wakeupExists(done.id, "k", ScopePath.empty), "a terminal instance must not be woken")
    } finally runner.stop(1.second)
  }

  test("a send or completion never creates a wakeup for a terminal subscriber") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "liv-terminal") { in => s"out-$in" }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("out-in"))
    // A stale signal subscription left on the terminal instance.
    run(
      sql"""
        INSERT INTO workflow_subscriptions (workflow_id, workflow_instance_key, workflow_instance_scope, step_id, step_scope_within_instance, step_version, subscriber_key, kind, signal_key)
        VALUES (${wf.id}, 'k', '{}', 'w', '{}', 0, '', 'signal', 'greet')
      """.update.run
    )
    deleteWakeup(wf.id, "k", ScopePath.empty)

    assertEquals(signal.send(id, "hi")(using rt), SignalSendResult.InstanceAlreadyCompleted)
    assert(!wakeupExists(wf.id, "k", ScopePath.empty), "a send must not wake a terminal addressee")

    // A terminal parent still holding a completion subscription on a child that then completes.
    val child = Workflow[String, Int]("liv-terminal-child") { in => in.length }
    val parent = Workflow[String, String]("liv-terminal-parent") { _ =>
      val c = child.startAsChild("c", "hi")
      Step.await[WorkflowCompletionResult[Int]]("await-child", c.completion)
      "done"
    }
    val parentId = rt.createWorkflowInstance(parent, "p", "in").id
    assertEquals(rt.runWorkflowInstance(parent, parentId), WorkflowRunResult.WorkflowSuspended)
    rt.terminate(parentId)
    deleteWakeup(parent.id, "p", ScopePath.empty)
    val childId = rt.getChildWorkflowInstances(parentId).head.id
    assertEquals(rt.runWorkflowInstance(child, childId), WorkflowRunResult.Result(2))
    assert(!wakeupExists(parent.id, "p", ScopePath.empty), "a completion must not wake a terminal subscriber")
  }

  test("one send to the parent wakes every currently eligible suspended child") {
    val rt = newRuntime
    val signal = Signal[String]("order/approve")
    val childWf = Workflow[String, String]("liv-eligible-child") { _ =>
      Step.await[String]("wait", Awaitable.SignalEvent(signal))
      "done"
    }
    val parentWf = Workflow[String, String]("liv-eligible-orders") { _ =>
      childWf.startAsChild("c1", "hi", inheritSignals = SignalInheritance.all)
      childWf.startAsChild("c2", "hi", inheritSignals = SignalInheritance.all)
      Step.await[String]("pwait", Awaitable.SignalEvent(Signal[String]("pgate")))
      "done"
    }
    val parentId = rt.createWorkflowInstance(parentWf, "o1", "in").id
    assertEquals(rt.runWorkflowInstance(parentWf, parentId), WorkflowRunResult.WorkflowSuspended)
    val c1 = rt.getChildWorkflowInstances(parentId).find(_.id.workflowInstanceKey == "c1").get.id
    val c2 = rt.getChildWorkflowInstances(parentId).find(_.id.workflowInstanceKey == "c2").get.id
    assertEquals(rt.runWorkflowInstance(childWf, c1), WorkflowRunResult.WorkflowSuspended)
    assertEquals(rt.runWorkflowInstance(childWf, c2), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(childWf.id, "c1", c1.scope)
    deleteWakeup(childWf.id, "c2", c2.scope)
    assert(!wakeupExists(childWf.id, "c1", c1.scope))
    assert(!wakeupExists(childWf.id, "c2", c2.scope))

    signal.send(parentId, "x")(using rt)
    assert(wakeupExists(childWf.id, "c1", c1.scope), "an eligible suspended child is woken by the parent send")
    assert(wakeupExists(childWf.id, "c2", c2.scope), "an eligible suspended child is woken by the parent send")
    assertEquals(rt.runWorkflowInstance(childWf, c1), WorkflowRunResult.Result("done"))
    assertEquals(rt.runWorkflowInstance(childWf, c2), WorkflowRunResult.Result("done"))
  }

  test("repeated full scans are idempotent: at most one wakeup row per instance") {
    val rt = newRuntime
    val signal = Signal[String]("greet")
    val wf = Workflow[String, String](id = "liv-idem") { _ =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(signal))
      s"got:$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    signal.send(id, "hi")(using rt)
    deleteWakeup(wf.id, "k", ScopePath.empty)

    val bystander = Workflow[String, String](id = "liv-bystander") { in => s"out-$in" }
    val runner = rt.startJobRunnerForTests(
      Seq(bystander),
      JobRunnerSettings.forTests.copy(fullSubscriptionScanInterval = 0.millis)
    )
    try {
      runner.runDriverCycle()
      runner.runDriverCycle()
      assertEquals(wakeupCount(wf.id, "k", ScopePath.empty), 1, "repeated scans must not accumulate duplicate wakeups")
      assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.Result("got:hi"))
    } finally runner.stop(1.second)
  }

  test("a send creates the addressee's wakeup only when it holds a subscription for that key") {
    val rt = newRuntime
    val awaited = Signal[String]("awaited")
    val unawaited = Signal[String]("unawaited")
    val wf = Workflow[String, String](id = "liv-writer") { _ =>
      val v = Step.await[String]("w", Awaitable.SignalEvent(awaited))
      s"got:$v"
    }
    val id = rt.createWorkflowInstance(wf, "k", "in").id
    assertEquals(rt.runWorkflowInstance(wf, id), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(wf.id, "k", ScopePath.empty)

    awaited.send(id, "hi")(using rt)
    assert(wakeupExists(wf.id, "k", ScopePath.empty), "the wakeup exists as soon as send returns")

    // An instance with no subscription for the sent key gets no wakeup.
    val other = Workflow[String, String](id = "liv-writer-other") { _ =>
      Step.await[String]("w", Awaitable.SignalEvent(Signal[String]("something-else")))
      "done"
    }
    val otherId = rt.createWorkflowInstance(other, "k", "in").id
    assertEquals(rt.runWorkflowInstance(other, otherId), WorkflowRunResult.WorkflowSuspended)
    deleteWakeup(other.id, "k", ScopePath.empty)
    unawaited.send(otherId, "x")(using rt)
    assert(!wakeupExists(other.id, "k", ScopePath.empty), "no subscription for the key: no wakeup")
  }
}
