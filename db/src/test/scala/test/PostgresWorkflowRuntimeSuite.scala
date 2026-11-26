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

  // Every round trip to the test database is slow in some environments (a
  // remote Docker daemon: ~177 ms), so a test of a few dozen transactions plus
  // the post-test checks can exceed munit's 30 s default without anything
  // being wrong.
  override val munitTimeout: scala.concurrent.duration.Duration = 120.seconds

  private lazy val dataSource: PGSimpleDataSource = SharedPostgres.newDatabase()

  override def beforeEach(context: BeforeEach): Unit = {
    super.beforeEach(context)
    SharedPostgres.truncateAll(dataSource)
  }

  /** Checks run after every passing test body, as part of the test (munit
    * does not report failures thrown from `afterEach`). Subclasses may add
    * checks; call `super`.
    */
  protected def afterTestChecks(): Unit = checkDeadlocks()

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "post-test checks",
      t => t.withBodyMap(_.map { value => afterTestChecks(); value }(munitExecutionContext))
    )

  /** Logs the suite database's deadlock count, so a full run's log carries it
    * (a failure here would not be reported by munit).
    */
  override def afterAll(): Unit = {
    println(s"[deadlocks] ${getClass.getName}: ${deadlocks()}")
    super.afterAll()
  }

  /** Fails when the suite's database recorded a deadlock not yet reported,
    * including one `withDeadlockRetry` retried away (Postgres counts every
    * one). The database is fresh per suite, so the count starts at 0; a count
    * whose statistics flush lags is reported by the next test.
    */
  private var deadlocksReported: Long = 0L

  private def checkDeadlocks(): Unit = {
    val now = deadlocks()
    if (now > deadlocksReported) {
      val fresh = now - deadlocksReported
      deadlocksReported = now
      fail(s"the database recorded $fresh new deadlock(s)")
    }
  }

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
    * direct or inherited signal past the cursor, a completed target, a timer at
    * or past its deadline that no run has handled — or no subscription at all
    * after it has run. A created,
    * never-run instance without a wakeup is excluded: `createWorkflowInstance`
    * does not schedule by contract (manual mode), so that state is not stuck.
    * Returns `(workflow id, key, scope, reason)`.
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
                -- An inherited signal: an event on an eligible ancestor (each edge
                -- permits the key, the ancestor is non-terminal and has not
                -- closed the child's scope) after the cursor and, unless
                -- inherit_past_events, after the inherited start sequence id.
                WITH RECURSIVE anc(k, pfx, pwf, pkey, pscope, node_scope, src_wf, src_key, src_scope, depth) AS (
                  SELECT s.signal_key, i.inherited_signal_prefixes, i.parent_workflow_id, i.parent_instance_key,
                         i.parent_instance_scope, i.workflow_instance_scope,
                         NULL::varchar, NULL::varchar, NULL::varchar[], 0
                  FROM workflow_subscriptions s
                  WHERE s.kind = 'signal' AND s.workflow_id = i.workflow_id AND s.workflow_instance_key = i.workflow_instance_key
                    AND s.workflow_instance_scope = i.workflow_instance_scope
                  UNION ALL
                  SELECT a.k, p.inherited_signal_prefixes, p.parent_workflow_id, p.parent_instance_key,
                         p.parent_instance_scope, p.workflow_instance_scope,
                         p.workflow_id, p.workflow_instance_key, p.workflow_instance_scope, a.depth + 1
                  FROM anc a
                  JOIN workflow_instances p
                    ON p.workflow_id = a.pwf AND p.workflow_instance_key = a.pkey AND p.workflow_instance_scope = a.pscope
                  WHERE p.terminal_state IS NULL
                    AND EXISTS (SELECT 1 FROM unnest(a.pfx) AS x(pf) WHERE starts_with(a.k, x.pf))
                    AND NOT EXISTS (
                      SELECT 1 FROM child_close_requests r
                      WHERE r.workflow_id = p.workflow_id AND r.workflow_instance_key = p.workflow_instance_key
                        AND r.workflow_instance_scope = p.workflow_instance_scope
                        AND a.node_scope[1:array_length(r.scope_prefix, 1)] = r.scope_prefix)
                )
                SELECT 1 FROM anc a
                JOIN workflow_events e
                  ON e.workflow_id = a.src_wf AND e.workflow_instance_key = a.src_key
                 AND e.workflow_instance_scope = a.src_scope AND e.event_key = a.k
                WHERE a.depth > 0
                  AND e.sequence_id > COALESCE((
                    SELECT c.sequence_id FROM signal_cursor c
                    WHERE c.workflow_id = i.workflow_id AND c.workflow_instance_key = i.workflow_instance_key
                      AND c.workflow_instance_scope = i.workflow_instance_scope AND c.signal_key = a.k), 0)
                  AND (i.inherit_past_events OR i.inherited_events_start_sequence_id IS NULL
                       OR e.sequence_id > i.inherited_events_start_sequence_id))
                THEN 'inherited signal past cursor'
              WHEN EXISTS (
                SELECT 1 FROM workflow_subscriptions s
                JOIN workflow_instances t
                  ON t.workflow_id = s.completed_workflow_id AND t.workflow_instance_key = s.completed_workflow_instance_key
                 AND t.workflow_instance_scope = s.completed_workflow_instance_scope AND t.terminal_state IS NOT NULL
                WHERE s.kind = 'completion' AND s.workflow_id = i.workflow_id AND s.workflow_instance_key = i.workflow_instance_key
                  AND s.workflow_instance_scope = i.workflow_instance_scope)
                THEN 'completion present'
              WHEN EXISTS (
                SELECT 1 FROM workflow_subscriptions s
                WHERE s.kind = 'timer' AND s.deadline <= now()
                  AND (i.last_run_at IS NULL OR i.last_run_at < s.deadline) AND s.workflow_id = i.workflow_id
                  AND s.workflow_instance_key = i.workflow_instance_key AND s.workflow_instance_scope = i.workflow_instance_scope)
                THEN 'timer due'
              WHEN i.times_executed > 0 AND NOT EXISTS (
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

  /** The current generation of an instance (default: top-level scope). Child
    * scopes embed it as `key@generation`; it is drawn from a sequence, so tests
    * read it instead of assuming a literal.
    */
  protected def generationOf(workflowId: String, key: String, scope: Vector[String] = Vector.empty): Long =
    run(
      sql"""
        SELECT generation FROM workflow_instances
        WHERE workflow_id = $workflowId AND workflow_instance_key = $key AND workflow_instance_scope = $scope
      """.query[Long].unique
    )

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
