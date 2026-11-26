package test

import atomicflow.impl.db.PostgresWorkflowRuntime
import cats.effect.IO
import doobie.ConnectionIO
import doobie.Transactor
import doobie.implicits.*
import doobie.postgres.implicits.*
import munit.FunSuite
import org.postgresql.ds.PGSimpleDataSource

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import java.time.Clock

/** Shared database setup for PostgreSQL runtime tests.
  *
  * Hands out a suite-specific database from the JVM-wide container in
  * [[SharedPostgres]] and empties all tables before each test; the schema
  * itself is never recreated. `newRuntime` uses the same database.
  */
abstract class PostgresWorkflowRuntimeSuite extends FunSuite {

  private lazy val dataSource: PGSimpleDataSource = SharedPostgres.newDatabase()

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    SharedPostgres.truncateAll(dataSource)
    deadlocksBefore = deadlocks()
  }

  /** Fails the test when the database recorded a deadlock during it, including
    * one that `withDeadlockRetry` retried away (Postgres counts every one).
    * The runtime opens a connection per transaction, so a victim's backend
    * exits and flushes its statistics right away.
    */
  override def afterEach(context: AfterEach): Unit = {
    val during = deadlocks() - deadlocksBefore
    super.afterEach(context)
    assertEquals(during, 0L, s"the database recorded $during deadlock(s) during this test")
  }

  private var deadlocksBefore: Long = 0L

  private def deadlocks(): Long = {
    val c = dataSource.getConnection
    try {
      val rs = c.createStatement().executeQuery(
        "SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()"
      )
      rs.next()
      rs.getLong(1)
    } finally c.close()
  }

  /** Instances nothing will ever run again: non-terminal, no wakeup row, no
    * live lease (against the database clock), and either a reason to run — a
    * direct signal past the cursor, a present completion, a timer at or past
    * its deadline — or no subscription at all. Returns
    * `(workflow id, key, scope, reason)`.
    */
  protected def stuckInstances(): Vector[(String, String, Vector[String], String)] =
    run(
      sql"""
        SELECT * FROM (
          SELECT i.workflow_id, i.workflow_instance_key, i.workflow_instance_scope,
            CASE
              WHEN EXISTS (
                SELECT 1 FROM workflow_subscriptions s
                JOIN workflow_events e
                  ON e.workflow_id = s.workflow_id AND e.workflow_instance_key = s.workflow_instance_key
                 AND e.workflow_instance_scope = s.workflow_instance_scope
                 AND e.event_kind = 'Signal' AND e.event_key = s.signal_key
                WHERE s.kind = 'signal' AND s.workflow_id = i.workflow_id AND s.workflow_instance_key = i.workflow_instance_key
                  AND s.workflow_instance_scope = i.workflow_instance_scope
                  AND e.sequence_id > COALESCE((
                    SELECT c.sequence_id FROM signal_cursor c
                    WHERE c.workflow_id = s.workflow_id AND c.workflow_instance_key = s.workflow_instance_key
                      AND c.workflow_instance_scope = s.workflow_instance_scope AND c.signal_key = s.signal_key), 0))
                THEN 'signal past cursor'
              WHEN EXISTS (
                SELECT 1 FROM workflow_subscriptions s
                JOIN workflow_events e
                  ON e.workflow_id = s.completed_workflow_id AND e.workflow_instance_key = s.completed_workflow_instance_key
                 AND e.workflow_instance_scope = s.completed_workflow_instance_scope AND e.event_kind = 'WorkflowCompleted'
                WHERE s.kind = 'completion' AND s.workflow_id = i.workflow_id AND s.workflow_instance_key = i.workflow_instance_key
                  AND s.workflow_instance_scope = i.workflow_instance_scope)
                THEN 'completion present'
              WHEN EXISTS (
                SELECT 1 FROM workflow_subscriptions s
                WHERE s.kind = 'timer' AND s.deadline <= now() AND s.workflow_id = i.workflow_id
                  AND s.workflow_instance_key = i.workflow_instance_key AND s.workflow_instance_scope = i.workflow_instance_scope)
                THEN 'timer due'
              WHEN NOT EXISTS (
                SELECT 1 FROM workflow_subscriptions s
                WHERE s.workflow_id = i.workflow_id AND s.workflow_instance_key = i.workflow_instance_key
                  AND s.workflow_instance_scope = i.workflow_instance_scope)
                THEN 'no subscription'
            END AS reason
          FROM workflow_instances i
          WHERE i.terminal_state IS NULL
            AND (i.lease_expires_at IS NULL OR i.lease_expires_at <= now())
            AND NOT EXISTS (
              SELECT 1 FROM workflow_wakeups w
              WHERE w.workflow_id = i.workflow_id AND w.workflow_instance_key = i.workflow_instance_key
                AND w.workflow_instance_scope = i.workflow_instance_scope)
        ) stuck
        WHERE reason IS NOT NULL
        ORDER BY 1, 2, 3
      """.query[(String, String, Vector[String], String)].to[Vector]
    )

  private def newDataSource: PGSimpleDataSource = dataSource

  /** The suite's data source, so a test in package `atomicflow` can construct
    * the package-private `PostgresWorkflowRuntime` directly (e.g. with a
    * fault-injecting constructor argument).
    */
  protected def dataSourceForTests: PGSimpleDataSource = dataSource

  private lazy val xa: Transactor[IO] =
    Transactor.fromDataSource[IO](newDataSource, ExecutionContext.global)

  /** Runs a doobie `ConnectionIO` program synchronously against the suite's DB. */
  protected def run[A](program: ConnectionIO[A]): A =
    program.transact(xa).unsafeRunSync()(using cats.effect.unsafe.IORuntime.global)

  /** A fresh runtime backed by the suite's single container. */
  def newRuntime: PostgresWorkflowRuntime =
    PostgresWorkflowRuntime(newDataSource)(using ExecutionContext.global)

  /** A raw JDBC connection to the suite's container, for tests that need to
    * manipulate row locks outside the runtime (e.g. hold a child's row with
    * `FOR UPDATE`).
    */
  protected def rawConnection: java.sql.Connection =
    newDataSource.getConnection

  /** A fresh runtime with an injected clock, backed by the suite's single container. */
  def newRuntime(clock: Clock): PostgresWorkflowRuntime =
    PostgresWorkflowRuntime(newDataSource, clock)(using ExecutionContext.global)

  /** A fresh runtime with injected clock, lease settings, and durable-retry
    * threshold (so tests can force inline vs. durable retries deterministically).
    */
  def newRuntime(
      clock: Clock,
      leaseDuration: FiniteDuration = 5.minutes,
      leaseAcquireTimeout: FiniteDuration = 30.seconds,
      durableRetryThreshold: FiniteDuration = 30.seconds
  ): PostgresWorkflowRuntime =
    PostgresWorkflowRuntime(newDataSource, clock, leaseDuration, leaseAcquireTimeout, durableRetryThreshold)(using
      ExecutionContext.global
    )
}
