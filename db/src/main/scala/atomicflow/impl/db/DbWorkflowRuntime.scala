package atomicflow.impl.db

import atomicflow.*
import atomicflow.Constants.libraryVersion
import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.impl.db.DbWorkflowRuntime.given
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore, StepInputFingerprints, StepScope, WorkflowScope}
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

import java.time.Instant
import java.util
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

class DbWorkflowRuntime[F[_] : Async](xa: Transactor[F], dispatcher: Dispatcher[F]) extends WorkflowRuntime with WorkflowRuntime.GenerateIds {

  override def createWorkflowInstance[In, Out](
                                                workflowInstance: WorkflowInstanceBuilder[In, Out],
                                                in: In
                                              )(
                                                using Cacheable[In]
                                              ): Unit = {
    val id = workflowInstance.instanceId
    val workflowId = workflowInstance.workflow.meta.id
    val workflowMeta = workflowInstance.workflow.meta
    val input = Cacheable[In].serialize(in).asInstanceOf[Array[Byte]]

    runSync {
      sql"SELECT input FROM workflow_instance WHERE id = $id"
        .query[Array[Byte]]
        .option
        .flatMap {
          case Some(prevInput) if util.Arrays.equals(prevInput, input) =>
            Monad[ConnectionIO].unit

          case Some(_) =>
            throw new WorkflowInputConflictException(
              workflowMeta,
              id
            )

          case None =>
            sql"INSERT INTO workflow_instance (id, workflow_id, input) VALUES ($id, $workflowId, $input)"
              .update
              .run
              .void
        }
    }
  }

  override def runWorkflowInstance[In, Out](
                                             workflowInstance: WorkflowInstanceBuilder[In, Out],
                                             in: In
                                           )(
                                             using Cacheable[In]
                                           ): Out = {
    createWorkflowInstance(workflowInstance, in)
    recoverWorkflowInstance(workflowInstance)
  }

  override def recoverWorkflowInstance[In, Out](
                                                 workflowInstance: WorkflowInstanceBuilder[In, Out]
                                               )(
                                                 using Cacheable[In]
                                               ): Out = {
    val id = workflowInstance.instanceId

    val lockDuration = java.time.Duration.ofMinutes(5)
    val now = Instant.now()
    val lockUntil = now.plus(lockDuration)
    val workflowMeta = workflowInstance.workflow.meta

    runSync {
      sql"SELECT id, locked_until FROM workflow_instance where id = $id"
        .query[(WorkflowInstanceId, Option[Instant])]
        .option
        .flatMap {
          case None =>
            throw new WorkflowNotFoundException(
              workflowMeta,
              id
            )

          case Some((_, Some(lockedUntil))) if now.isBefore(lockedUntil) =>
            throw new WorkflowLockedException(
              workflowMeta,
              id
            )

          case Some((_, _)) =>
            sql"UPDATE workflow_instance SET locked_until = $lockUntil WHERE id = $id"
              .update
              .run
              .void
        }
    }

    val ctx = new atomicflow.WorkflowContext[In, Out] {
      override val meta: WorkflowMeta = workflowInstance.workflow.meta

      override val instanceId: WorkflowInstanceId = workflowInstance.instanceId

      override protected[atomicflow] def getFingerprinter: Fingerprinter =
        atomicflow.impl.Sha256Fingerprinter

      override protected[atomicflow] def getStepIdempotencyStore(stepScope: StepScope): StepIdempotencyStore.Bound =
        new DbStepIdempotencyStore(stepScope, workflowInstance.stepIdempotencyIdOverrides)

      override protected[atomicflow] def getStepCache[StepOut: Cacheable](stepScope: StepScope): StepCache.Bound[StepOut] =
        new DbStepCache[StepOut](stepScope)

      override protected[atomicflow] def getSignalStore: SignalStore.Bound =
        DbSignalStore.bind(workflowScope)

      override protected[atomicflow] val defaultCacheTtl: FiniteDuration =
        workflowInstance.defaultCacheTtl
    }

    try {
      val inputBytes = runSync(loadInput(id)).getOrElse {
        throw new WorkflowNotFoundException(
          workflowMeta,
          id
        )
      }
      workflowInstance.workflow.body(ctx, Cacheable[In].deserialize(inputBytes.asInstanceOf[IArray[Byte]]))
    } finally {
      val unlock =
        sql"""
        UPDATE workflow_instance
        SET locked_until = NULL
        WHERE id = $id
      """.update.run
      runSync(unlock)
    }
  }

  private def loadInput(id: WorkflowInstanceId): ConnectionIO[Option[Array[Byte]]] =
    sql"SELECT input FROM workflow_instance WHERE id = $id".query[Array[Byte]].option

  class DbStepIdempotencyStore(
                                stepScope: StepScope,
                                stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
                              ) extends StepIdempotencyStore.Bound {
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

    override def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId = {
      val idQuery = sql"""
        SELECT id FROM step_idempotency
          WHERE workflow_id = ${stepScope.workflowExecutionScope.workflowMeta.id} AND
            workflow_instance_id = ${stepScope.workflowExecutionScope.workflowInstanceId} AND
              step_id = ${stepScope.stepMeta.id} AND
              is_only_once = true AND
              is_overridden = false
      """.query[StepIdempotencyId].option

      def insertQuery(id: StepIdempotencyId) =
        sql"""
          INSERT INTO step_idempotency (id, library_version, workflow_id, workflow_instance_id, step_id, is_only_once)
          VALUES ($id, $libraryVersion, ${stepScope.workflowExecutionScope.workflowMeta.id}, ${stepScope.workflowExecutionScope.workflowInstanceId}, ${stepScope.stepMeta.id}, true)
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

      runSync {
        idQuery.flatMap {
          case Some(existing) =>
            stepIdempotencyIdOverrides.get(stepScope.stepMeta.id) match {
              case Some(overrideId) if overrideId != existing =>
                updateQuery >>
                  insertQuery(overrideId)
              case _ =>
                Monad[ConnectionIO].pure(existing)
            }
          case None =>
            val stepIdempotencyId = stepIdempotencyIdOverrides.getOrElse(
              stepScope.stepMeta.id,
              StepIdempotencyId.unsafeMake(UUID.randomUUID().toString)
            )
            insertQuery(stepIdempotencyId)
        }
      }
    }
  }

  class DbStepCache[Out: Cacheable](stepScope: StepScope) extends StepCache.Bound[Out] {
    override def get(
                      stepIdempotencyId: StepIdempotencyId,
                      inputFingerprints: StepInputFingerprints
                    ): Option[Out] = {
      val query = sql"""
        SELECT output, step_version, input_fingerprints FROM step_cache
        WHERE step_idempotency_id = ${stepIdempotencyId} and step_id = ${stepScope.stepMeta.id}
      """.query[(Array[Byte], Long, StepInputFingerprints)].option

      runSync(query).flatMap {
        case (data, version, fingerprints) if version == stepScope.stepMeta.version && fingerprints == inputFingerprints =>
          Some(Cacheable[Out].deserialize(data.asInstanceOf[IArray[Byte]]))
        case _ => throw stepScope.stepInputConflictException()
      }
    }

    override def put(
                      stepIdempotencyId: StepIdempotencyId,
                      inputFingerprints: StepInputFingerprints,
                      value: Out,
                      ttl: FiniteDuration
                    ): Unit = {
      val expiry = java.time.Instant.now().plusMillis(ttl.toMillis)
      val data = Cacheable[Out].serialize(value).asInstanceOf[Array[Byte]]

      val existingQuery =
        sql"""
        SELECT step_version, input_fingerprints FROM step_cache
        WHERE step_idempotency_id = ${stepIdempotencyId} and step_id = ${stepScope.stepMeta.id}
      """.query[(Long, StepInputFingerprints)].option

      val query =
        sql"""
        INSERT INTO step_cache (step_idempotency_id, step_id, step_version, input_fingerprints, output, expiry)
        VALUES (${stepIdempotencyId}, ${stepScope.stepMeta.id}, ${stepScope.stepMeta.version}, $inputFingerprints, $data, $expiry)
        ON CONFLICT (step_idempotency_id) DO UPDATE
        SET step_version = ${stepScope.stepMeta.version},
            input_fingerprints = $inputFingerprints,
            output = $data,
            expiry = $expiry
      """.update.run.void

      runSync(existingQuery).foreach {
        case (existingVersion, existingFingerprints)
          if existingVersion != stepScope.stepMeta.version || existingFingerprints != inputFingerprints =>
          throw stepScope.stepInputConflictException()
        case _ =>
      }

      runSync(query)
    }
  }

  object DbSignalStore extends SignalStore {

    // TODO: check expiry
    private def select[A](workflowScope: WorkflowScope, signal: Signal[A]): ConnectionIO[Option[Array[Byte]]] =
      sql"""
      SELECT value FROM workflow_signals
      WHERE id=${signal.meta.id} AND workflow_id=${workflowScope.workflowMeta.id} AND workflow_instance_id=${workflowScope.workflowInstanceId}
      """.query[Array[Byte]].option

    override def bind(workflowScope: WorkflowScope): SignalStore.Bound = new SignalStore.Bound {
      override def getSignalValue[A](signal: Signal[A]): Option[A] =
        runSync {
          select(workflowScope, signal)
        }.map { bytes =>
          signal.cacheable.deserialize(bytes.asInstanceOf[IArray[Byte]])
        }

      @throws[SignalConflictException]
      override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration): Unit = {
        val expiry = java.time.Instant.now().plusMillis(ttl.toMillis)
        val bytes: Array[Byte] = signal.cacheable.serialize(value).asInstanceOf[Array[Byte]]

        runSync {
          select(workflowScope, signal).flatMap {
            case Some(prevBytes) if util.Arrays.equals(prevBytes, bytes) =>
              Monad[ConnectionIO].unit

            case Some(_) =>
              throw new SignalConflictException(
                signal,
                workflowScope.workflowMeta,
                workflowScope.workflowInstanceId
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
              """.update.run.map {
                  case 0 => throw new WorkflowNotFoundException(
                    workflowScope.workflowMeta,
                    workflowScope.workflowInstanceId
                  )
                case 1 => ()
              }
          }
        }
      }
    }
  }

  private def runSync[A](fa: ConnectionIO[A]): A =
    dispatcher.unsafeRunSync(fa.transact(xa))

  @throws[SignalConflictException]
  override def setSignal[A](signal: Signal[A], value: A, ttl: FiniteDuration)(using workflowCtx: SimpleWorkflowContext): Unit =
    DbSignalStore
      .bind(WorkflowScope(workflowCtx.meta, workflowCtx.instanceId))
      .setSignalValue(signal, value, ttl)
}

object DbWorkflowRuntime {
  case class DbConfig(
                       driver: Option[String],
                       url: String,
                       user: String,
                       password: String,
                       poolSize: Option[Int]
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

  def apply(config: DbConfig): WorkflowRuntime = {
    (for {
      dispatcher <- Dispatcher.parallel[IO]
      xa <- transactor(config)
    } yield
      new DbWorkflowRuntime[IO](xa, dispatcher))
      .allocated.map(_._1)
      .unsafeRunSync()(cats.effect.unsafe.IORuntime.global)
  }

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
