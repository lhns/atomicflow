package atomicflow.impl.memory

import atomicflow.*
import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.impl.Sha256Fingerprinter
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore, StepInputFingerprints, StepScope, StepState, WorkflowScope}

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.FiniteDuration

/** @param retryBackoff base delay of the exponential backoff for retrying failed runs via wakeups */
class InMemoryWorkflowRuntime(
                               retryBackoff: FiniteDuration = FiniteDuration(5, java.util.concurrent.TimeUnit.SECONDS)
                             ) extends WorkflowRuntime with WorkflowRuntime.GenerateIds {
  class WorkflowIdempotencyStore {
    sealed trait IdempotencyIdKey

    case class StepIdempotencyIdKey(stepId: StepId, stepVersion: Long, inputs: StepInputFingerprints) extends IdempotencyIdKey

    case class OnceStepIdempotencyIdKey(stepId: StepId) extends IdempotencyIdKey

    val idempotencyIds: AtomicReference[Map[IdempotencyIdKey, StepIdempotencyId]] = new AtomicReference(Map.empty)

    def bind(
              stepScope: StepScope,
              stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
            ): StepIdempotencyStore = new StepIdempotencyStore {
      override def acquireStepIdempotencyId(inputFingerprints: StepInputFingerprints): StepIdempotencyId = {
        val key = StepIdempotencyIdKey(stepScope.stepMeta.id, stepScope.stepMeta.version, inputFingerprints)
        idempotencyIds.updateAndGet(ids => ids.get(key) match {
          case Some(_) => ids
          case None =>
            val id = generateStepIdempotencyId
            ids + (key -> id)
        })(key)
      }

      override def acquireOnlyOnceStepIdempotencyId(): StepIdempotencyId = {
        val key = OnceStepIdempotencyIdKey(stepScope.stepMeta.id)
        stepIdempotencyIdOverrides.get(stepScope.stepMeta.id) match {
          case Some(idempotencyId) =>
            idempotencyIds.updateAndGet(_ + (key -> idempotencyId))
            idempotencyId
          case None =>
            idempotencyIds.updateAndGet(ids => ids.get(key) match {
              case Some(_) => ids
              case None =>
                val id = generateStepIdempotencyId
                ids + (key -> id)
            })(key)
        }
      }
    }
  }

  class WorkflowStepCache {
    /** Stored in place of a value while an at-most-once step's body runs. */
    private object StartedMarker

    val stepCache: AtomicReference[Map[StepIdempotencyId, (StepId, Long, StepInputFingerprints, Any)]] = new AtomicReference(Map.empty)

    def bind[StepOut](stepScope: StepScope): StepCache[StepOut] = new StepCache[StepOut] {
      private val stepId = stepScope.stepMeta.id
      private val stepVersion = stepScope.stepMeta.version

      override def get(
                        stepIdempotencyId: StepIdempotencyId,
                        inputFingerprints: StepInputFingerprints
                      ): StepState[StepOut] =
        stepCache.get().get(stepIdempotencyId) match {
          case None => StepState.NotStarted
          case Some((`stepId`, `stepVersion`, `inputFingerprints`, StartedMarker)) => StepState.Started
          case Some((`stepId`, `stepVersion`, `inputFingerprints`, out: StepOut @unchecked)) => StepState.Completed(out)
          case Some(_) => throw stepScope.stepConflictError()
        }

      override def markStarted(
                                stepIdempotencyId: StepIdempotencyId,
                                inputFingerprints: StepInputFingerprints
                              ): Unit =
        stepCache.updateAndGet(_ + (stepIdempotencyId -> (stepId, stepVersion, inputFingerprints, StartedMarker)))

      override def clearStarted(stepIdempotencyId: StepIdempotencyId): Unit =
        stepCache.updateAndGet { cache =>
          cache.get(stepIdempotencyId) match {
            case Some((_, _, _, StartedMarker)) => cache - stepIdempotencyId
            case _ => cache
          }
        }

      override def put(
                        stepIdempotencyId: StepIdempotencyId,
                        inputFingerprints: StepInputFingerprints,
                        value: StepOut,
                        ttl: FiniteDuration
                      ): Unit =
        stepCache.updateAndGet { cache =>
          cache.get(stepIdempotencyId) match {
            case Some((existingStepId, existingStepVersion, existingFingerprints, _))
              if existingStepId != stepId || existingStepVersion != stepVersion || existingFingerprints != inputFingerprints =>
              throw stepScope.stepConflictError()

            case _ =>
              cache + (stepIdempotencyId -> (stepId, stepVersion, inputFingerprints, value))
          }
        }
    }
  }

  private case class WorkflowState[In, Out](
                                             locked: AtomicBoolean,
                                             in: In,
                                             workflow: Workflow[In, Out],
                                             defaultCacheTtl: FiniteDuration,
                                             stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
                                             stepCache: WorkflowStepCache,
                                             stepIdempotencyStore: WorkflowIdempotencyStore,
                                             rootWorkflowId: WorkflowId,
                                             rootInstanceId: WorkflowInstanceId,
                                             versionAtCreation: Int
                                           )

  private val workflowInstances: AtomicReference[Map[WorkflowInstanceId, WorkflowState[?, ?]]] = new AtomicReference(Map.empty)

  override def createWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    in: In,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
    parent: Option[WorkflowContext]
  ): Unit = {
    workflowInstances.updateAndGet { instances =>
      instances.get(instanceId) match {
        case Some(state) =>
          if (state.in != in) {
            throw WorkflowError.InputConflict(
              workflow.meta,
              instanceId
            )
          } else {
            instances
          }
        case None =>
          if (parent.isEmpty) wakeups.schedule(workflow.meta.id, instanceId, Instant.now(), claimedBy = None, resetAttempts = true, ifAbsent = true)
          instances + (instanceId -> WorkflowState(
            locked = new AtomicBoolean(false),
            in = in,
            workflow = workflow,
            defaultCacheTtl = defaultCacheTtl,
            stepIdempotencyIdOverrides = stepIdempotencyIdOverrides,
            stepCache = new WorkflowStepCache(),
            stepIdempotencyStore = new WorkflowIdempotencyStore(),
            rootWorkflowId = parent.fold(workflow.meta.id)(_.rootWorkflowId),
            rootInstanceId = parent.fold(instanceId)(_.rootInstanceId),
            versionAtCreation = workflow.meta.version
          ))
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
    workflowInstances.get().get(instanceId) match {
      case Some(state: WorkflowState[In, Out] @unchecked) =>
        if (state.locked.getAndSet(true)) {
          throw WorkflowError.Locked(
            workflow.meta,
            instanceId
          )
        } else {
          val isRoot = state.rootInstanceId == instanceId
          val claimToken = UUID.randomUUID()
          if (isRoot) wakeups.schedule(workflow.meta.id, instanceId, Instant.now().plusMillis(wakeupClaimTimeout.toMillis), claimedBy = Some(claimToken), resetAttempts = false, ifAbsent = false)
          var outcome: Option[Throwable] = Some(new IllegalStateException("run did not finish"))
          try {
            val _instanceId = instanceId
            val _defaultCacheTtl = defaultCacheTtl
            val _parent = parent
            val ctx = new WorkflowContext {
              override val meta: WorkflowMeta = workflow.meta

              override val instanceId: WorkflowInstanceId = _instanceId

              override protected[atomicflow] def runtime: WorkflowRuntime = InMemoryWorkflowRuntime.this

              override protected[atomicflow] def getFingerprinter: Fingerprinter = Sha256Fingerprinter

              override protected[atomicflow] def getStepIdempotencyStore(stepScope: StepScope): StepIdempotencyStore =
                state.stepIdempotencyStore.bind(stepScope, stepIdempotencyIdOverrides)

              override protected[atomicflow] def getStepCache[StepOut: Cacheable](stepScope: StepScope): StepCache[StepOut] =
                state.stepCache.bind[StepOut](stepScope)

              override protected[atomicflow] def getSignalStore: SignalStore =
                signalStore.bind(workflowScope)

              override protected[atomicflow] val defaultCacheTtl: FiniteDuration =
                _defaultCacheTtl

              override protected[atomicflow] val parent: Option[WorkflowContext] = _parent

              override protected[atomicflow] val rootWorkflowId: WorkflowId = state.rootWorkflowId

              override protected[atomicflow] val rootInstanceId: WorkflowInstanceId = state.rootInstanceId

              override protected[atomicflow] val versionAtCreation: Int = state.versionAtCreation
            }
            val result =
              try workflow.body(ctx, state.in)
              catch {
                case e: Throwable =>
                  outcome = Some(e)
                  throw e
              }
            outcome = None
            result
          } finally {
            state.locked.set(false)
            if (isRoot) wakeups.completeRun(instanceId, claimToken, WorkflowRuntime.isFinished(outcome))
          }
        }

      case _ =>
        throw WorkflowError.NotFound(
          workflow.meta,
          instanceId
        )
    }
  }

  private class WorkflowSignalStore {
    // Stored serialized, like a persistent runtime would, so readers observe exactly what a replay observes.
    val signalValues: AtomicReference[Map[(WorkflowId, WorkflowInstanceId, SignalId), IArray[Byte]]] = new AtomicReference(Map.empty)

    def bind(workflowScope: WorkflowScope): SignalStore = new SignalStore {
      override def getSignalValue[A](signal: Signal[A]): Option[A] = {
        val key = (workflowScope.workflowMeta.id, workflowScope.workflowInstanceId, signal.meta.id)
        signalValues.get().get(key).map(signal.cacheable.deserialize)
      }

      override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration): Unit = {
        val key = (workflowScope.workflowMeta.id, workflowScope.workflowInstanceId, signal.meta.id)

        if (!workflowInstances.get().contains(workflowScope.workflowInstanceId))
          throw WorkflowError.NotFound(
            workflowScope.workflowMeta,
            workflowScope.workflowInstanceId
          )

        val bytes = signal.cacheable.serialize(value)
        signalValues.updateAndGet { map =>
          if (map.get(key).exists(existing => !java.util.Arrays.equals(existing.asInstanceOf[Array[Byte]], bytes.asInstanceOf[Array[Byte]])))
            throw WorkflowError.SignalConflict(
              workflowScope.workflowMeta,
              workflowScope.workflowInstanceId,
              signal
            )

          map + (key -> bytes)
        }
      }
    }
  }

  private val signalStore = new WorkflowSignalStore

  /** In-memory state dies with the process, so claimed wakeups only need to outlive a crashed worker thread. */
  private val wakeupClaimTimeout: FiniteDuration = FiniteDuration(5, java.util.concurrent.TimeUnit.MINUTES)

  private case class WakeupState(workflowId: WorkflowId, scheduledAt: Instant, attempts: Int, claimedBy: Option[UUID])

  private object wakeups {
    private var states: Map[WorkflowInstanceId, WakeupState] = Map.empty

    def schedule(
                  workflowId: WorkflowId,
                  instanceId: WorkflowInstanceId,
                  at: Instant,
                  claimedBy: Option[UUID],
                  resetAttempts: Boolean,
                  ifAbsent: Boolean
                ): Unit = synchronized {
      states.get(instanceId) match {
        case Some(_) if ifAbsent => ()
        case existing =>
          val attempts = if (resetAttempts) 0 else existing.fold(0)(_.attempts)
          states += instanceId -> WakeupState(workflowId, at, attempts, claimedBy)
      }
    }

    /** A finished run removes its wakeup; a failed run retries with backoff. Either only applies if the wakeup was not
      * touched during the run (e.g. by a signal, which must lead to another pass). */
    def completeRun(instanceId: WorkflowInstanceId, claimToken: UUID, finished: Boolean): Unit = synchronized {
      states.get(instanceId) match {
        case Some(state) if state.claimedBy.contains(claimToken) =>
          if (finished) states -= instanceId
          else states += instanceId -> state.copy(
            scheduledAt = Instant.now().plusMillis(WorkflowRuntime.retryDelay(retryBackoff, state.attempts).toMillis),
            attempts = state.attempts + 1,
            claimedBy = None
          )
        case _ => ()
      }
    }

    def claim(workflowIds: Set[WorkflowId], limit: Int): Seq[WorkflowRuntime.Wakeup] = synchronized {
      val now = Instant.now()
      val due = states.toSeq
        .filter { case (_, state) => workflowIds.contains(state.workflowId) && !state.scheduledAt.isAfter(now) }
        .sortBy(_._2.scheduledAt)
        .take(limit)
      due.foreach { case (instanceId, state) =>
        states += instanceId -> state.copy(scheduledAt = now.plusMillis(wakeupClaimTimeout.toMillis))
      }
      due.map { case (instanceId, state) => WorkflowRuntime.Wakeup(state.workflowId, instanceId, state.attempts) }
    }
  }

  override def claimWakeups(workflowIds: Set[WorkflowId], limit: Int): Seq[WorkflowRuntime.Wakeup] =
    wakeups.claim(workflowIds, limit)

  override def scheduleWakeup(workflowId: WorkflowId, instanceId: WorkflowInstanceId, delay: FiniteDuration): Unit =
    wakeups.schedule(workflowId, instanceId, Instant.now().plusMillis(delay.toMillis), claimedBy = None, resetAttempts = false, ifAbsent = false)

  override def setSignal[A](
                             signal: Signal[A],
                             value: A,
                             ttl: FiniteDuration,
                             workflowMeta: WorkflowMeta,
                             workflowInstanceId: WorkflowInstanceId
                           ): Unit = {
    signalStore
      .bind(WorkflowScope(workflowMeta, workflowInstanceId))
      .setSignalValue(signal, value, ttl)
    workflowInstances.get().get(workflowInstanceId).foreach { state =>
      wakeups.schedule(state.rootWorkflowId, state.rootInstanceId, Instant.now(), claimedBy = None, resetAttempts = true, ifAbsent = false)
    }
  }
}
