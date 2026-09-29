package test

import atomicflow.{*, given}
import atomicflow.Cacheable.Simple.given
import atomicflow.impl.db.DbWorkflowRuntime
import atomicflow.impl.db.DbWorkflowRuntime.DbConfig

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

class DbWorkflowRuntimeSuite extends WorkflowRuntimeSuite {
  private val requiredDbVars = List("DB_URL", "DB_USERNAME", "DB_PASSWORD")

  private val missingDbVars = requiredDbVars.filterNot(name => sys.env.get(name).exists(_.nonEmpty))

  private val skipReason =
    s"Missing required DB env vars for DbWorkflowRuntimeSuite: ${missingDbVars.mkString(", ")}"

  override def munitIgnore: Boolean = missingDbVars.nonEmpty

  private lazy val dbConfig: DbConfig = DbConfig(
    driver = None,
    url = sys.env("DB_URL"),
    user = sys.env("DB_USERNAME"),
    password = sys.env("DB_PASSWORD"),
    poolSize = None
  )

  private object UnavailableRuntime extends WorkflowRuntime with WorkflowRuntime.GenerateIds {
    private def unavailable[A]: A =
      throw new IllegalStateException(skipReason)

    override def createWorkflowInstance[In: Cacheable, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId,
      in: In,
      defaultCacheTtl: FiniteDuration,
      stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
      parent: Option[WorkflowContext]
    ): Unit =
      unavailable

    override def runWorkflowInstance[In: Cacheable, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId,
      in: In,
      defaultCacheTtl: FiniteDuration,
      stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
      parent: Option[WorkflowContext]
    ): Out =
      unavailable

    override def recoverWorkflowInstance[In: Cacheable, Out](
      workflow: Workflow[In, Out],
      instanceId: WorkflowInstanceId,
      defaultCacheTtl: FiniteDuration,
      stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
      parent: Option[WorkflowContext]
    ): Out =
      unavailable

    override def claimWakeups(workflowIds: Set[WorkflowId], limit: Int): Seq[WorkflowRuntime.Wakeup] =
      unavailable

    override def scheduleWakeup(workflowId: WorkflowId, instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit =
      unavailable

    override def setSignal[A](
                               signal: Signal[A],
                               value: A,
                               ttl: FiniteDuration,
                               workflowMeta: WorkflowMeta,
                               workflowInstanceId: WorkflowInstanceId
                             ): Unit =
      unavailable
  }

  override def createWorkflowRuntime: WorkflowRuntime =
    if (missingDbVars.nonEmpty) UnavailableRuntime
    else DbWorkflowRuntime(dbConfig)

  override def createWorkflowRuntime(retryBackoff: FiniteDuration): WorkflowRuntime =
    if (missingDbVars.nonEmpty) UnavailableRuntime
    else DbWorkflowRuntime(dbConfig.copy(retryBackoff = retryBackoff))

  // DB-only: lock expiry, takeover and renewal need real lock timeouts.
  private lazy val shortLockRuntime: WorkflowRuntime = DbWorkflowRuntime(dbConfig.copy(lockTimeout = 2.seconds))

  private def thread(body: => Unit): Thread = {
    val t = new Thread(() => body)
    t.start()
    t
  }

  test("An expired lock can be taken over; the previous owner can neither write step results nor release it") {
    given WorkflowRuntime = shortLockRuntime
    val bodyCalls = AtomicInteger(0)
    val bInSecondStep = new CountDownLatch(1)
    val bRelease = new CountDownLatch(1)

    val workflow = Workflow["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a01"]("lock takeover")[Unit, Int] { _ =>
      val a = Step.cached["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a02", 0]() {
        val call = bodyCalls.incrementAndGet()
        if (call == 1) Thread.sleep(3000) // run A outlives its 2s lock without reaching a checkpoint
        call
      }
      Step.cached["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a03", 0]("a" -> a) {
        if (a == 2) {
          bInSecondStep.countDown()
          bRelease.await()
        }
        a
      }
    }

    val instanceId = WorkflowInstanceId.generate
    workflow.create(instanceId)

    val resultA = AtomicReference[Try[Int]]()
    val resultB = AtomicReference[Try[Int]]()
    val threadA = thread(resultA.set(Try(workflow.recover(instanceId))))
    Thread.sleep(2300)
    val threadB = thread(resultB.set(Try(workflow.recover(instanceId))))
    assert(bInSecondStep.await(5, java.util.concurrent.TimeUnit.SECONDS), "run B did not take over the expired lock")

    threadA.join()
    assert(resultA.get().failed.toOption.exists(_.isInstanceOf[WorkflowError.Locked]), s"run A: ${resultA.get()}")

    // A's release must not have freed B's lock
    intercept[WorkflowError.Locked] {
      workflow.recover(instanceId)
    }

    bRelease.countDown()
    threadB.join()
    assertEquals(resultB.get().get, 2)
    assertEquals(workflow.recover(instanceId), 2)
    assertEquals(bodyCalls.get(), 2)
  }

  test("A running instance renews its lock at checkpoints") {
    given WorkflowRuntime = shortLockRuntime

    val workflow = Workflow["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a04"]("lock renewal")[Unit, Int] { _ =>
      (1 to 5).map { i =>
        Step.cached["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a05", 0]("i" -> i) {
          Thread.sleep(700)
          i
        }
      }.sum
    }

    val instanceId = WorkflowInstanceId.generate
    workflow.create(instanceId)

    val result = AtomicReference[Try[Int]]()
    val runner = thread(result.set(Try(workflow.recover(instanceId))))
    Thread.sleep(2600) // past the initial 2s lock
    intercept[WorkflowError.Locked] {
      workflow.recover(instanceId)
    }
    runner.join()
    assertEquals(result.get().get, 15)
  }

  test("After a lost lock, an in-flight onlyOnce step is reported as unknown instead of running twice") {
    given WorkflowRuntime = shortLockRuntime
    val effects = AtomicInteger(0)

    val workflow = Workflow["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a06"]("lock takeover once")[Unit, Int] { _ =>
      Step.onlyOnce["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a07"]() {
        effects.incrementAndGet()
        Thread.sleep(3000) // outlives the 2s lock
        1
      }
    }

    val instanceId = WorkflowInstanceId.generate
    workflow.create(instanceId)

    val resultA = AtomicReference[Try[Int]]()
    val threadA = thread(resultA.set(Try(workflow.recover(instanceId))))
    Thread.sleep(2300)
    intercept[WorkflowError.StepUnknownState] {
      workflow.recover(instanceId)
    }
    threadA.join()
    assert(resultA.get().failed.toOption.exists(_.isInstanceOf[WorkflowError.Locked]), s"run A: ${resultA.get()}")
    assertEquals(effects.get(), 1)
  }

  test("A child running inline keeps its ancestors' locks alive") {
    given WorkflowRuntime = shortLockRuntime

    val child = Workflow["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a08"]("long child")[Unit, Int] { _ =>
      (1 to 5).map { i =>
        Step.cached["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a09", 0]("i" -> i) {
          Thread.sleep(700)
          i
        }
      }.sum
    }

    val parent = Workflow["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a10"]("parent of long child")[Unit, Int] { _ =>
      val result = child.runChild("c")
      // A fenced parent write after the child: fails with Locked if the parent's lock was taken over meanwhile.
      Step.cached["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a11", 0]("result" -> result) {
        result
      }
    }

    val parentId = WorkflowInstanceId.generate
    parent.create(parentId)

    val result = AtomicReference[Try[Int]]()
    val runner = thread(result.set(Try(parent.recover(parentId))))
    Thread.sleep(2600) // past the parent's initial 2s lock, while the child is still running
    intercept[WorkflowError.Locked] {
      parent.recover(parentId)
    }
    runner.join()
    assertEquals(result.get().get, 15)
  }

  test("A worker picks up a run that stopped making progress once its lock expired") {
    given WorkflowRuntime = shortLockRuntime
    val calls = AtomicInteger(0)

    val workflow = Workflow["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a12"]("stalled run")[Unit, Int] { _ =>
      Step.cached["c3a1e0d2-6f4b-4d8e-9b1a-7e2c5f3d0a13", 0]() {
        if (calls.incrementAndGet() == 1) Thread.sleep(4000) // a stalled (or, in production, dead) run
        calls.get()
      }
    }

    val worker = WorkflowWorker(Seq(workflow))
    val instanceId = WorkflowInstanceId.generate
    workflow.create(instanceId)

    val stalled = thread { Try(workflow.recover(instanceId)); () }
    Thread.sleep(500)
    assertEquals(worker.runOnce(), 0) // the running root's wakeup is postponed by the lock timeout
    Thread.sleep(2000)
    assertEquals(worker.runOnce(), 1) // lock expired: the worker takes over and completes the run
    assertEquals(workflow.recover(instanceId), 2)
    stalled.join()
  }
}
