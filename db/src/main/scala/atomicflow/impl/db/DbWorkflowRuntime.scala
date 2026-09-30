package atomicflow.impl.db

import atomicflow.*
import atomicflow.Constants.libraryVersion
import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.impl.db.CirceCodecs.given
import atomicflow.impl.db.DbWorkflowRuntime.given
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore, StepInputFingerprints, StepScope, StepState, WorkflowScope}
import cats.Monad
import cats.effect.std.Dispatcher
import cats.effect.{Async, IO, Resource}
import cats.syntax.all.*
import de.lhns.doobie.flyway.BaselineMigrations.*
import de.lhns.doobie.flyway.Flyway
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import doobie.postgres.circe.jsonb.implicits.*
import doobie.postgres.implicits.*

import java.time.{Clock, Instant}
import java.util
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

class DbWorkflowRuntime[F[_] : Async](
                                        xa: Transactor[F],
                                        dispatcher: Dispatcher[F],
                                        lockTimeout: FiniteDuration = DbWorkflowRuntime.defaultLockTimeout,
                                        retryBackoff: FiniteDuration = DbWorkflowRuntime.defaultRetryBackoff,
                                        clock: Option[Clock] = None
                                      ) extends WorkflowRuntime with WorkflowRuntime.GenerateIds {

  /** The current time for all time-based decisions (lock expiry, wakeups, retries), inlined into queries: the
    * database's time by default, so decisions do not depend on each node's clock, or the injected clock (e.g. a
    * controllable test clock). */
  private def now: Fragment =
    clock.fold(fr0"now()")(clock => fr0"${Instant.now(clock)}::timestamptz")

  private def currentInstant: Instant =
    clock.fold(Instant.now())(Instant.now(_))

  override def createWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    in: In,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
    parent: Option[WorkflowContext]
  ): Unit = {
    val workflowId = workflow.meta.id
    val workflowMeta = workflow.meta
    val input = Cacheable[In].serialize(in).asInstanceOf[Array[Byte]]

    runSync {
      sql"SELECT input FROM workflow_instance WHERE id = $instanceId"
        .query[Array[Byte]]
        .option
        .flatMap {
          case Some(prevInput) if util.Arrays.equals(prevInput, input) =>
            Monad[ConnectionIO].unit

          case Some(_) =>
            throw WorkflowError.InputConflict(
              workflowMeta,
              instanceId
            )

          case None =>
            val rootWorkflowId = parent.fold(workflowId)(_.rootWorkflowId)
            val rootInstanceId = parent.fold(instanceId)(_.rootInstanceId)
            sql"""
            INSERT INTO workflow_instance (id, workflow_id, input, root_workflow_id, root_instance_id, workflow_version)
            VALUES ($instanceId, $workflowId, $input, $rootWorkflowId, $rootInstanceId, ${workflow.meta.version})
            """
              .update
              .run
              .void >>
              (if (parent.isEmpty) scheduleWakeupNow(workflowId, instanceId, ifAbsent = true) else Monad[ConnectionIO].unit)
        }
    }
  }

  override def runWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    in: In,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
    parent: Option[WorkflowContext]
  ): Out = {
    createWorkflowInstance(workflow, instanceId, in, defaultCacheTtl, stepIdempotencyIdOverrides, parent)
    recoverWorkflowInstance(workflow, instanceId, defaultCacheTtl, stepIdempotencyIdOverrides, parent)
  }

  override def recoverWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
    parent: Option[WorkflowContext]
  ): Out = {
    val workflowMeta = workflow.meta
    val lockToken = UUID.randomUUID()

    val Acquired(input, storedRoot, storedVersion) = runSync {
      acquireLock(instanceId, lockToken).flatMap {
        case Some(acquired @ Acquired(_, root, _)) =>
          // Crash safety for roots: until this run finishes, its wakeup is postponed by the lock timeout, so a run
          // interrupted by a dead process is retried once its lock has expired.
          if (root.forall(_._2 == instanceId)) claimOwnWakeup(workflowMeta.id, instanceId, lockToken).as(acquired)
          else Monad[ConnectionIO].pure(acquired)
        case None =>
          sql"SELECT 1 FROM workflow_instance WHERE id = $instanceId".query[Int].option.map {
            case None => throw WorkflowError.NotFound(workflowMeta, instanceId)
            case Some(_) => throw WorkflowError.Locked(workflowMeta, instanceId)
          }
      }
    }

    val _instanceId = instanceId
    val _defaultCacheTtl = defaultCacheTtl
    val _parent = parent
    val ctx = new atomicflow.WorkflowContext {
      override val meta: WorkflowMeta = workflowMeta

      override val instanceId: WorkflowInstanceId = _instanceId

      override protected[atomicflow] def runtime: WorkflowRuntime = DbWorkflowRuntime.this

      override protected[atomicflow] def getFingerprinter: Fingerprinter =
        atomicflow.impl.Sha256Fingerprinter

      override protected[atomicflow] def getStepIdempotencyStore(stepScope: StepScope): StepIdempotencyStore =
        new DbStepIdempotencyStore(stepScope, stepIdempotencyIdOverrides)

      override protected[atomicflow] def getStepCache[StepOut: Cacheable](stepScope: StepScope): StepCache[StepOut] =
        new DbStepCache[StepOut](stepScope, lockToken)

      override protected[atomicflow] def getSignalStore: SignalStore =
        DbSignalStore.bind(workflowScope)

      override protected[atomicflow] val defaultCacheTtl: FiniteDuration =
        _defaultCacheTtl

      override protected[atomicflow] val parent: Option[atomicflow.WorkflowContext] = _parent

      // Instances created before root links existed are their own root.
      override protected[atomicflow] val rootWorkflowId: WorkflowId = storedRoot.fold(workflowMeta.id)(_._1)

      override protected[atomicflow] val rootInstanceId: WorkflowInstanceId = storedRoot.fold(_instanceId)(_._2)

      override protected[atomicflow] val versionAtCreation: Int = storedVersion

      @volatile private var lockRenewedAt: Instant = currentInstant

      override protected[atomicflow] def isCancelRequested: Boolean =
        runSync {
          sql"SELECT cancel_requested_at IS NOT NULL FROM workflow_instance WHERE id = $_instanceId".query[Boolean].unique
        }

      override protected[atomicflow] def renewLock(): Unit =
        if (currentInstant.isAfter(lockRenewedAt.plusMillis(lockTimeout.toMillis / 2))) {
          val renewedAt = currentInstant
          val renewed = runSync {
            sql"""
            UPDATE workflow_instance
            SET locked_until = $now + make_interval(secs => $lockTimeoutSeconds)
            WHERE id = $_instanceId AND lock_owner = $lockToken
            """.update.run
          }
          if (renewed == 0) throw WorkflowError.Locked(workflowMeta, _instanceId)
          lockRenewedAt = renewedAt
        }
    }

    var outcome: Option[Throwable] = Some(new IllegalStateException("run did not finish"))
    try {
      val result =
        try workflow.body(ctx, Cacheable[In].deserialize(input.asInstanceOf[IArray[Byte]]))
        catch {
          case e: Throwable =>
            outcome = Some(e)
            throw e
        }
      outcome = None
      result
    } finally {
      // Only release the lock if we still own it: after expiry another run may have taken it over.
      runSync {
        sql"""
        UPDATE workflow_instance
        SET locked_until = NULL, lock_owner = NULL
        WHERE id = $instanceId AND lock_owner = $lockToken
        """.update.run >>
          completeOwnWakeup(instanceId, lockToken, WorkflowRuntime.isFinished(outcome))
      }
    }
  }

  private val lockTimeoutSeconds: Double = lockTimeout.toMillis / 1000.0

  /** What a run learns about its instance when it acquires the lock. `root` is not recorded for instances created
    * before root links existed. */
  private case class Acquired(input: Array[Byte], root: Option[(WorkflowId, WorkflowInstanceId)], versionAtCreation: Int)

  /** Atomically acquires the execution lock if it is free or expired. */
  private def acquireLock(
                           instanceId: WorkflowInstanceId,
                           lockToken: UUID
                         ): ConnectionIO[Option[Acquired]] =
    sql"""
    UPDATE workflow_instance
    SET locked_until = $now + make_interval(secs => $lockTimeoutSeconds), lock_owner = $lockToken
    WHERE id = $instanceId AND (locked_until IS NULL OR locked_until < $now)
    RETURNING input, root_workflow_id, root_instance_id, workflow_version
    """.query[(Array[Byte], Option[WorkflowId], Option[WorkflowInstanceId], Int)].option.map(_.map {
      case (input, rootWorkflowId, rootInstanceId, version) => Acquired(input, rootWorkflowId.zip(rootInstanceId), version)
    })

  private val retryBackoffSeconds: Double = retryBackoff.toMillis / 1000.0

  private val maxRetryDelaySeconds: Double = WorkflowRuntime.maxRetryDelay.toMillis / 1000.0

  private def scheduleWakeupNow(workflowId: WorkflowId, instanceId: WorkflowInstanceId, ifAbsent: Boolean): ConnectionIO[Unit] =
    (sql"""
    INSERT INTO workflow_wakeup (root_instance_id, root_workflow_id, scheduled_at, attempts, claim_token)
    VALUES ($instanceId, $workflowId, $now, 0, NULL)
    ON CONFLICT (root_instance_id) DO """ ++
      (if (ifAbsent) fr"NOTHING" else fr"UPDATE SET scheduled_at = $now, attempts = 0, claim_token = NULL")
      ).update.run.void

  private def claimOwnWakeup(workflowId: WorkflowId, instanceId: WorkflowInstanceId, lockToken: UUID): ConnectionIO[Unit] =
    sql"""
    INSERT INTO workflow_wakeup (root_instance_id, root_workflow_id, scheduled_at, attempts, claim_token)
    VALUES ($instanceId, $workflowId, $now + make_interval(secs => $lockTimeoutSeconds), 0, $lockToken)
    ON CONFLICT (root_instance_id) DO UPDATE
    SET scheduled_at = EXCLUDED.scheduled_at, claim_token = EXCLUDED.claim_token
    """.update.run.void

  /** A finished run removes its wakeup; a failed run retries with backoff. Either only applies if the wakeup was not
    * touched during the run (e.g. by a signal, which must lead to another pass). */
  private def completeOwnWakeup(instanceId: WorkflowInstanceId, lockToken: UUID, finished: Boolean): ConnectionIO[Unit] =
    if (finished)
      sql"DELETE FROM workflow_wakeup WHERE root_instance_id = $instanceId AND claim_token = $lockToken".update.run.void
    else
      sql"""
      UPDATE workflow_wakeup
      SET scheduled_at = $now + make_interval(secs => LEAST($retryBackoffSeconds * power(2, LEAST(attempts, 30)), $maxRetryDelaySeconds)),
          attempts = attempts + 1,
          claim_token = NULL
      WHERE root_instance_id = $instanceId AND claim_token = $lockToken
      """.update.run.void

  override def cancelWorkflowInstance(workflowMeta: WorkflowMeta, instanceId: WorkflowInstanceId): Unit =
    runSync {
      sql"""
      UPDATE workflow_instance
      SET cancel_requested_at = COALESCE(cancel_requested_at, $now)
      WHERE id = $instanceId
      """.update.run.flatMap {
        case 0 => throw WorkflowError.NotFound(workflowMeta, instanceId)
        case _ => wakeRoot(instanceId)
      }
    }

  override def claimWakeups(workflowIds: Set[WorkflowId], limit: Int): Seq[WorkflowRuntime.Wakeup] =
    if (workflowIds.isEmpty) Seq.empty
    else {
      val ids: Array[UUID] = workflowIds.toArray.map(id => UUID.fromString(WorkflowId.unwrap(id)))
      runSync {
        sql"""
        UPDATE workflow_wakeup
        SET scheduled_at = $now + make_interval(secs => $lockTimeoutSeconds)
        WHERE root_instance_id IN (
          SELECT root_instance_id FROM workflow_wakeup
          WHERE scheduled_at <= $now AND root_workflow_id = ANY($ids)
          ORDER BY scheduled_at
          LIMIT $limit
          FOR UPDATE SKIP LOCKED
        )
        RETURNING root_workflow_id, root_instance_id, attempts
        """.query[(WorkflowId, WorkflowInstanceId, Int)].to[List]
      }.map { case (workflowId, instanceId, attempts) => WorkflowRuntime.Wakeup(workflowId, instanceId, attempts) }
    }

  override def scheduleWakeup(workflowId: WorkflowId, instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit =
    runSync {
      sql"""
      INSERT INTO workflow_wakeup (root_instance_id, root_workflow_id, scheduled_at, attempts, claim_token)
      VALUES ($instanceId, $workflowId, $now + make_interval(secs => ${delay.toMillis / 1000.0}), 0, NULL)
      ON CONFLICT (root_instance_id) DO UPDATE
      SET scheduled_at = EXCLUDED.scheduled_at, claim_token = NULL
      """.update.run.void
    }

  /** Guards writes of step state: they only apply while the given lock token still owns the instance. */
  private def ownsLock(instanceId: WorkflowInstanceId, lockToken: UUID): Fragment =
    fr"EXISTS (SELECT 1 FROM workflow_instance WHERE id = $instanceId AND lock_owner = $lockToken)"

  class DbStepIdempotencyStore(
                                stepScope: StepScope,
                                stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
                              ) extends StepIdempotencyStore {
    override def acquireStepIdempotencyId(inputFingerprints: StepInputFingerprints): StepIdempotencyId = {
      val idQuery = sql"""
        SELECT id FROM step_idempotency
        WHERE library_version = $libraryVersion AND
              workflow_id = ${stepScope.workflowExecutionScope.workflowMeta.id} AND
              workflow_instance_id = ${stepScope.workflowExecutionScope.workflowInstanceId} AND
              step_id = ${stepScope.stepMeta.id} AND
              step_version = ${stepScope.stepMeta.version} AND
              input_fingerprints = $inputFingerprints
      """.query[StepIdempotencyId].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_id, step_id, step_version, input_fingerprints, is_only_once)
          VALUES ($id, $libraryVersion, ${stepScope.workflowExecutionScope.workflowMeta.id}, ${stepScope.workflowExecutionScope.workflowInstanceId}, ${stepScope.stepMeta.id}, ${stepScope.stepMeta.version}, $inputFingerprints, false)
          ON CONFLICT DO NOTHING
        """.update.run.as(id)

      runSync {
        idQuery.flatMap {
          case Some(existing) => Monad[ConnectionIO].pure(existing)
          case None =>
            val stepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)
            insertQuery(stepIdempotencyId)
        }
      }
    }

    override def acquireOnlyOnceStepIdempotencyId(keyFingerprints: StepInputFingerprints): StepIdempotencyId = {
      val idQuery = sql"""
        SELECT id, input_fingerprints FROM step_idempotency
          WHERE workflow_id = ${stepScope.workflowExecutionScope.workflowMeta.id} AND
            workflow_instance_id = ${stepScope.workflowExecutionScope.workflowInstanceId} AND
              step_id = ${stepScope.stepMeta.id} AND
              is_only_once = true AND
              is_overridden = false
          FOR UPDATE
      """.query[(StepIdempotencyId, Option[StepInputFingerprints])].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_id, step_id, input_fingerprints, is_only_once)
          VALUES ($id, $libraryVersion, ${stepScope.workflowExecutionScope.workflowMeta.id}, ${stepScope.workflowExecutionScope.workflowInstanceId}, ${stepScope.stepMeta.id}, $keyFingerprints, true)
          ON CONFLICT DO NOTHING
        """.update.run.as(id)

      val updateQuery: ConnectionIO[Unit] =
        sql"""
        UPDATE step_idempotency
        SET is_overridden = true
          WHERE workflow_id = ${stepScope.workflowExecutionScope.workflowMeta.id} AND
            workflow_instance_id = ${stepScope.workflowExecutionScope.workflowInstanceId} AND
            step_id = ${stepScope.stepMeta.id} AND
              is_only_once = true AND
              is_overridden = false
        """.update.run.void

      def newId(): StepIdempotencyId = StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)

      runSync {
        idQuery.flatMap {
          case Some((existing, recordedKeyFingerprints)) =>
            stepIdempotencyIdOverrides.get(stepScope.stepMeta.id) match {
              case Some(overrideId) if overrideId != existing =>
                updateQuery >>
                  insertQuery(overrideId)
              case Some(_) =>
                Monad[ConnectionIO].pure(existing)
              // rows from before drift policies have no recorded key fingerprints: nothing was invalidating
              case None if recordedKeyFingerprints.getOrElse(StepInputFingerprints(Map.empty)) != keyFingerprints =>
                updateQuery >>
                  insertQuery(newId())
              case None =>
                Monad[ConnectionIO].pure(existing)
            }
          case None =>
            insertQuery(stepIdempotencyIdOverrides.getOrElse(stepScope.stepMeta.id, newId()))
        }
      }
    }
  }

  class DbStepCache[Out: Cacheable](stepScope: StepScope, lockToken: UUID) extends StepCache[Out] {
    private val instanceId = stepScope.workflowExecutionScope.workflowInstanceId

    private def lockedError: WorkflowError.Locked =
      WorkflowError.Locked(stepScope.workflowExecutionScope.workflowMeta, instanceId)

    private def selectExisting(stepIdempotencyId: StepIdempotencyId): ConnectionIO[Option[(Option[Array[Byte]], Long, StepInputFingerprints)]] =
      sql"""
        SELECT output, step_version, input_fingerprints FROM step_cache
        WHERE step_idempotency_id = ${stepIdempotencyId} and step_id = ${stepScope.stepMeta.id}
      """.query[(Option[Array[Byte]], Long, StepInputFingerprints)].option

    private def matches(version: Long, fingerprints: StepInputFingerprints, inputFingerprints: StepInputFingerprints): Boolean =
      version == stepScope.stepMeta.version && fingerprints == inputFingerprints

    override def get(
                      stepIdempotencyId: StepIdempotencyId,
                      inputFingerprints: StepInputFingerprints
                    ): StepState[Out] =
      runSync(selectExisting(stepIdempotencyId)) match {
        case None => StepState.NotStarted
        case Some((output, version, fingerprints)) if matches(version, fingerprints, inputFingerprints) =>
          output.fold(StepState.Started)(data => StepState.Completed(Cacheable[Out].deserialize(data.asInstanceOf[IArray[Byte]])))
        case Some(_) => throw stepScope.stepConflictError()
      }

    override def markStarted(
                              stepIdempotencyId: StepIdempotencyId,
                              inputFingerprints: StepInputFingerprints
                            ): Unit =
      runSync {
        (sql"""
        INSERT INTO step_cache (step_idempotency_id, step_id, step_version, input_fingerprints, output, expiry)
        SELECT ${stepIdempotencyId}, ${stepScope.stepMeta.id}, ${stepScope.stepMeta.version}, $inputFingerprints, NULL, NULL
        WHERE """ ++ ownsLock(instanceId, lockToken)).update.run
      } match {
        case 0 => throw lockedError
        case _ => ()
      }

    override def clearStarted(stepIdempotencyId: StepIdempotencyId): Unit =
      runSync {
        (sql"""
        DELETE FROM step_cache
        WHERE step_idempotency_id = ${stepIdempotencyId} AND output IS NULL AND """ ++ ownsLock(instanceId, lockToken)).update.run
      }

    override def put(
                      stepIdempotencyId: StepIdempotencyId,
                      inputFingerprints: StepInputFingerprints,
                      value: Out,
                      ttl: FiniteDuration
                    ): Unit = {
      val expiry = currentInstant.plusMillis(ttl.toMillis)
      val data = Cacheable[Out].serialize(value).asInstanceOf[Array[Byte]]

      val query =
        (sql"""
        INSERT INTO step_cache (step_idempotency_id, step_id, step_version, input_fingerprints, output, expiry)
        SELECT ${stepIdempotencyId}, ${stepScope.stepMeta.id}, ${stepScope.stepMeta.version}, $inputFingerprints, $data, $expiry
        WHERE """ ++ ownsLock(instanceId, lockToken) ++ fr"""
        ON CONFLICT (step_idempotency_id) DO UPDATE
        SET step_version = ${stepScope.stepMeta.version},
            input_fingerprints = $inputFingerprints,
            output = $data,
            expiry = $expiry
      """).update.run

      runSync {
        selectExisting(stepIdempotencyId).flatMap {
          case Some((_, version, fingerprints)) if !matches(version, fingerprints, inputFingerprints) =>
            throw stepScope.stepConflictError()
          case _ =>
            query.map {
              case 0 => throw lockedError
              case _ => ()
            }
        }
      }
    }
  }

  object DbSignalStore {

    // TODO: check expiry
    private def select[A](workflowScope: WorkflowScope, signal: Signal[A]): ConnectionIO[Option[Array[Byte]]] =
      sql"""
      SELECT value FROM workflow_signals
      WHERE id=${signal.meta.id} AND workflow_id=${workflowScope.workflowMeta.id} AND workflow_instance_id=${workflowScope.workflowInstanceId}
      """.query[Array[Byte]].option

    def bind(workflowScope: WorkflowScope): SignalStore = new SignalStore {
      override def getSignalValue[A](signal: Signal[A]): Option[A] =
        runSync {
          select(workflowScope, signal)
        }.map { bytes =>
          signal.cacheable.deserialize(bytes.asInstanceOf[IArray[Byte]])
        }

      @throws[WorkflowError.SignalConflict]
      override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration): Unit = {
        val expiry = currentInstant.plusMillis(ttl.toMillis)
        val bytes: Array[Byte] = signal.cacheable.serialize(value).asInstanceOf[Array[Byte]]

        runSync {
          select(workflowScope, signal).flatMap {
            case Some(prevBytes) if util.Arrays.equals(prevBytes, bytes) =>
              Monad[ConnectionIO].unit

            case Some(_) =>
              throw WorkflowError.SignalConflict(
                workflowScope.workflowMeta,
                workflowScope.workflowInstanceId,
                signal
              )

            case None =>
              sql"""
              INSERT INTO workflow_signals (id, workflow_id, workflow_instance_id, value, expiry)
              SELECT ${signal.meta.id}, ${workflowScope.workflowMeta.id}, ${workflowScope.workflowInstanceId}, $bytes, $expiry
              WHERE EXISTS (
                SELECT 1
                FROM workflow_instance
                WHERE id = ${workflowScope.workflowInstanceId}
              )
              """.update.run.flatMap {
                  case 0 => throw WorkflowError.NotFound(
                    workflowScope.workflowMeta,
                    workflowScope.workflowInstanceId
                  )
                case _ => wakeRoot(workflowScope.workflowInstanceId)
              }
          }
        }
      }
    }
  }

  /** Wakes up the root of the given instance's tree (instances without a recorded root are their own root). */
  private def wakeRoot(instanceId: WorkflowInstanceId): ConnectionIO[Unit] =
    sql"""
    INSERT INTO workflow_wakeup (root_instance_id, root_workflow_id, scheduled_at, attempts, claim_token)
    SELECT COALESCE(root_instance_id, id), COALESCE(root_workflow_id, workflow_id::uuid), $now, 0, NULL
    FROM workflow_instance WHERE id = $instanceId
    ON CONFLICT (root_instance_id) DO UPDATE
    SET scheduled_at = $now, attempts = 0, claim_token = NULL
    """.update.run.void

  private def runSync[A](fa: ConnectionIO[A]): A =
    dispatcher.unsafeRunSync(fa.transact(xa))

  @throws[WorkflowError.SignalConflict]
  override def setSignal[A](signal: Signal[A], value: A, ttl: FiniteDuration, workflowMeta: WorkflowMeta, workflowInstanceId: WorkflowInstanceId): Unit =
    DbSignalStore
      .bind(WorkflowScope(workflowMeta, workflowInstanceId))
      .setSignalValue(signal, value, ttl)
}

object DbWorkflowRuntime {
  val defaultLockTimeout: FiniteDuration = FiniteDuration(5, java.util.concurrent.TimeUnit.MINUTES)

  /** Base delay of the exponential backoff for retrying failed runs via wakeups. */
  val defaultRetryBackoff: FiniteDuration = FiniteDuration(5, java.util.concurrent.TimeUnit.SECONDS)

  /** @param lockTimeout how long an execution lock stays valid without renewal. Runs renew it at every checkpoint
    *                    (before new step work), so a single step body must finish within this timeout.
    * @param clock       time source for locks, wakeups and retries; `None` uses the database's time. */
  case class DbConfig(
                       driver: Option[String],
                       url: String,
                       user: String,
                       password: String,
                       poolSize: Option[Int],
                       lockTimeout: FiniteDuration = defaultLockTimeout,
                       retryBackoff: FiniteDuration = defaultRetryBackoff,
                       clock: Option[Clock] = None
                     ) {
    def driverOrDefault: String = driver.getOrElse("org.postgresql.Driver")

    def poolSizeOrDefault: Int = poolSize.getOrElse(32)
  }

  private def transactor(config: DbConfig): Resource[IO, Transactor[IO]] =
    for {
      ce <- ExecutionContexts.fixedThreadPool[IO](config.poolSizeOrDefault)
      xa <- HikariTransactor
        .newHikariTransactor[IO](
          config.driverOrDefault,
          config.url,
          config.user,
          config.password,
          ce
        )
      _ <- Flyway(xa) { flyway =>
        for {
          info <- flyway.info()
          _ <- flyway
            .configure(_
              .withBaselineMigrate(info)
              .validateMigrationNaming(true)
            )
            .migrate()
        } yield ()
      }
    } yield xa

  /** The runtime as a resource: releasing it closes its connection pool. */
  def resource(config: DbConfig): Resource[IO, WorkflowRuntime] =
    for {
      dispatcher <- Dispatcher.parallel[IO]
      xa <- transactor(config)
    } yield
      new DbWorkflowRuntime[IO](xa, dispatcher, config.lockTimeout, config.retryBackoff, config.clock)

  /** Creates a runtime, together with a function that closes its connection pool. */
  def allocate(config: DbConfig): (WorkflowRuntime, () => Unit) = {
    given cats.effect.unsafe.IORuntime = cats.effect.unsafe.IORuntime.global
    val (runtime, release) = resource(config).allocated.unsafeRunSync()
    (runtime, () => release.unsafeRunSync())
  }

  /** Creates a runtime whose connection pool lives as long as the JVM. Use [[resource]] or [[allocate]] to close it. */
  def apply(config: DbConfig): WorkflowRuntime =
    allocate(config)._1

  given Meta[StepIdempotencyId] = Meta[UUID].imap(uuid =>
    StepIdempotencyId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(StepIdempotencyId.unwrap(id))
  )

  given Meta[StepId] = Meta[UUID].imap(uuid =>
    StepId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(StepId.unwrap(id))
  )

  given Meta[WorkflowId] = Meta[UUID].imap(uuid =>
    WorkflowId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(WorkflowId.unwrap(id))
  )

  given Meta[WorkflowInstanceId] = Meta[UUID].imap(uuid =>
    WorkflowInstanceId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(WorkflowInstanceId.unwrap(id))
  )

  given Meta[SignalId] = Meta[UUID].imap(uuid =>
    SignalId.unsafeMake(uuid.toString)
  )(id =>
    UUID.fromString(SignalId.unwrap(id))
  )

  given Get[StepInputFingerprints] = pgDecoderGetT[StepInputFingerprints]

  given Put[StepInputFingerprints] = pgEncoderPutT[StepInputFingerprints]
}
