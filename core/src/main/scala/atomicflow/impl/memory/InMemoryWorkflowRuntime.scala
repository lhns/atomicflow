package atomicflow.impl.memory

import atomicflow.*
import atomicflow.Fingerprintable.Fingerprinter
import atomicflow.impl.Sha256Fingerprinter
import atomicflow.internal.{SignalStore, StepCache, StepIdempotencyStore, StepInputFingerprints, StepScope, WorkflowScope}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.FiniteDuration

class InMemoryWorkflowRuntime extends WorkflowRuntime with WorkflowRuntime.GenerateIds {
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
    val stepCache: AtomicReference[Map[StepIdempotencyId, (StepId, Long, StepInputFingerprints, Any)]] = new AtomicReference(Map.empty)

    def bind[StepOut](stepScope: StepScope): StepCache[StepOut] = new StepCache[StepOut] {
      override def get(
                        stepIdempotencyId: StepIdempotencyId,
                        inputFingerprints: StepInputFingerprints
                      ): Option[StepOut] = {
        val stepId = stepScope.stepMeta.id
        val stepVersion = stepScope.stepMeta.version
        stepCache.get().get(stepIdempotencyId).map {
          case (`stepId`, `stepVersion`, `inputFingerprints`, out: StepOut @unchecked) => out
          case _ => throw stepScope.stepConflictError()
        }
      }


      override def put(
                        stepIdempotencyId: StepIdempotencyId,
                        inputFingerprints: StepInputFingerprints,
                        value: StepOut,
                        ttl: FiniteDuration
                      ): Unit = {
        val stepId = stepScope.stepMeta.id
        val stepVersion = stepScope.stepMeta.version

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
  }

  private case class WorkflowState[In, Out](
                                             locked: AtomicBoolean,
                                             in: In,
                                             workflow: Workflow[In, Out],
                                             defaultCacheTtl: FiniteDuration,
                                             stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId],
                                             stepCache: WorkflowStepCache,
                                             stepIdempotencyStore: WorkflowIdempotencyStore
                                           )

  private val workflowInstances: AtomicReference[Map[WorkflowInstanceId, WorkflowState[?, ?]]] = new AtomicReference(Map.empty)

  override def createWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    in: In,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
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
          instances + (instanceId -> WorkflowState(
            locked = new AtomicBoolean(false),
            in = in,
            workflow = workflow,
            defaultCacheTtl = defaultCacheTtl,
            stepIdempotencyIdOverrides = stepIdempotencyIdOverrides,
            stepCache = new WorkflowStepCache(),
            stepIdempotencyStore = new WorkflowIdempotencyStore()
          ))
      }
    }
  }

  override def runWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    in: In,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
  ): Out = {
    createWorkflowInstance(workflow, instanceId, in, defaultCacheTtl, stepIdempotencyIdOverrides)
    recoverWorkflowInstance(workflow, instanceId, defaultCacheTtl, stepIdempotencyIdOverrides)
  }

  override def recoverWorkflowInstance[In: Cacheable, Out](
    workflow: Workflow[In, Out],
    instanceId: WorkflowInstanceId,
    defaultCacheTtl: FiniteDuration,
    stepIdempotencyIdOverrides: Map[StepId, StepIdempotencyId]
  ): Out = {
    workflowInstances.get().get(instanceId) match {
      case Some(state: WorkflowState[In, Out] @unchecked) =>
        if (state.locked.getAndSet(true)) {
          throw WorkflowError.Locked(
            workflow.meta,
            instanceId
          )
        } else {
          try {
            val _instanceId = instanceId
            val _defaultCacheTtl = defaultCacheTtl
            val ctx = new WorkflowContext {
              override val meta: WorkflowMeta = workflow.meta

              override val instanceId: WorkflowInstanceId = _instanceId

              override protected[atomicflow] def getFingerprinter: Fingerprinter = Sha256Fingerprinter

              override protected[atomicflow] def getStepIdempotencyStore(stepScope: StepScope): StepIdempotencyStore =
                state.stepIdempotencyStore.bind(stepScope, stepIdempotencyIdOverrides)

              override protected[atomicflow] def getStepCache[StepOut: Cacheable](stepScope: StepScope): StepCache[StepOut] =
                state.stepCache.bind[StepOut](stepScope)

              override protected[atomicflow] def getSignalStore: SignalStore =
                signalStore.bind(workflowScope)

              override protected[atomicflow] val defaultCacheTtl: FiniteDuration =
                _defaultCacheTtl
            }
            workflow.body(ctx, state.in)
          } finally {
            state.locked.set(false)
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
    val signalValues: AtomicReference[Map[(WorkflowId, WorkflowInstanceId, SignalId), ?]] = new AtomicReference(Map.empty)

    def bind(workflowScope: WorkflowScope): SignalStore = new SignalStore {
      override def getSignalValue[A](signal: Signal[A]): Option[A] = {
        val key = (workflowScope.workflowMeta.id, workflowScope.workflowInstanceId, signal.meta.id)
        signalValues.get().get(key).asInstanceOf[Option[A]]
      }

      override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration): Unit = {
        val key = (workflowScope.workflowMeta.id, workflowScope.workflowInstanceId, signal.meta.id)

        if (!workflowInstances.get().contains(workflowScope.workflowInstanceId))
          throw WorkflowError.NotFound(
            workflowScope.workflowMeta,
            workflowScope.workflowInstanceId
          )

        signalValues.updateAndGet { map =>
          if (map.get(key).exists(_ != value))
            throw WorkflowError.SignalConflict(
              workflowScope.workflowMeta,
              workflowScope.workflowInstanceId,
              signal
            )

          map + (key -> value)
        }
      }
    }
  }

  private val signalStore = new WorkflowSignalStore

  override def setSignal[A](
                             signal: Signal[A],
                             value: A,
                             ttl: FiniteDuration,
                             workflowMeta: WorkflowMeta,
                             workflowInstanceId: WorkflowInstanceId
                           ): Unit =
    signalStore
      .bind(WorkflowScope(workflowMeta, workflowInstanceId))
      .setSignalValue(signal, value, ttl)
}
