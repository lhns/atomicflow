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
            ): StepIdempotencyStore.Bound = new StepIdempotencyStore.Bound {
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

    def bind[StepOut](stepScope: StepScope): StepCache.Bound[StepOut] = new StepCache.Bound[StepOut] {
      override def get(
                        stepIdempotencyId: StepIdempotencyId,
                        inputFingerprints: StepInputFingerprints
                      ): Option[StepOut] = {
        val stepId = stepScope.stepMeta.id
        val stepVersion = stepScope.stepMeta.version
        stepCache.get().get(stepIdempotencyId).map {
          case (`stepId`, `stepVersion`, `inputFingerprints`, out: StepOut @unchecked) => out
          case _ => throw stepScope.stepInputConflictException()
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
              throw stepScope.stepInputConflictException()

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
                                             workflowInstance: WorkflowInstanceBuilder[In, Out],
                                             stepCache: WorkflowStepCache,
                                             stepIdempotencyStore: WorkflowIdempotencyStore
                                           )

  private val workflowInstances: AtomicReference[Map[WorkflowInstanceId, WorkflowState[?, ?]]] = new AtomicReference(Map.empty)

  override def createWorkflowInstance[WorkflowIn, WorkflowOut](
                                                                workflowInstance: WorkflowInstanceBuilder[WorkflowIn, WorkflowOut],
                                                                in: WorkflowIn
                                                              )(
                                                                using Cacheable[WorkflowIn]
                                                              ): Unit = {
    given SimpleWorkflowContext {
      override def meta: WorkflowMeta = workflowInstance.workflow.meta

      override def instanceId: WorkflowInstanceId = workflowInstance.instanceId
    }

    workflowInstances.updateAndGet { instances =>
      instances.get(workflowInstance.instanceId) match {
        case Some(state) =>
          if (state.in != in) {
            throw new WorkflowInputConflictException()
          } else {
            instances
          }
        case None =>
          instances + (workflowInstance.instanceId -> WorkflowState(
            locked = new AtomicBoolean(false),
            in = in,
            workflowInstance = workflowInstance,
            stepCache = new WorkflowStepCache(),
            stepIdempotencyStore = new WorkflowIdempotencyStore()
          ))
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
    given SimpleWorkflowContext {
      override def meta: WorkflowMeta = workflowInstance.workflow.meta

      override def instanceId: WorkflowInstanceId = workflowInstance.instanceId
    }

    workflowInstances.get().get(workflowInstance.instanceId) match {
      case Some(state: WorkflowState[In, Out] @unchecked) =>
        if (state.locked.getAndSet(true)) {
          // was locked before
          throw new WorkflowLockedException()
        } else {
          // was not locked before
          try {
            val ctx = new WorkflowContext[In, Out] {
              override val meta: WorkflowMeta = workflowInstance.workflow.meta

              override val instanceId: WorkflowInstanceId = workflowInstance.instanceId

              override protected[atomicflow] def getFingerprinter: Fingerprinter = Sha256Fingerprinter

              override protected[atomicflow] def getStepIdempotencyStore(stepScope: StepScope): StepIdempotencyStore.Bound =
                state.stepIdempotencyStore.bind(stepScope, workflowInstance.stepIdempotencyIdOverrides)

              override protected[atomicflow] def getStepCache[StepOut: Cacheable](stepScope: StepScope): StepCache.Bound[StepOut] =
                state.stepCache.bind[StepOut](stepScope)

              override protected[atomicflow] def getSignalStore: SignalStore.Bound =
                signalStore.bind(workflowScope)

              override protected[atomicflow] val defaultCacheTtl: FiniteDuration =
                workflowInstance.defaultCacheTtl
            }
            state.workflowInstance.workflow.body(ctx, state.in)
          } finally {
            state.locked.set(false)
          }
        }

      case _ =>
        throw new WorkflowNotFoundException()
    }
  }

  private val signalStore: SignalStore = new SignalStore {
    val signalValues: AtomicReference[Map[(WorkflowId, WorkflowInstanceId, SignalId), ?]] = new AtomicReference(Map.empty)

    override def bind(workflowScope: WorkflowScope): SignalStore.Bound = new SignalStore.Bound {
      override def getSignalValue[A](signal: Signal[A]): Option[A] = {
        val key = (workflowScope.workflowMeta.id, workflowScope.workflowInstanceId, signal.meta.id)
        signalValues.get().get(key).asInstanceOf[Option[A]]
      }

      override def setSignalValue[A](signal: Signal[A], value: A, ttl: FiniteDuration): Unit = {
        val key = (workflowScope.workflowMeta.id, workflowScope.workflowInstanceId, signal.meta.id)
        given SimpleWorkflowContext = workflowScope.simpleWorkflowContext

        if (!workflowInstances.get().contains(workflowScope.workflowInstanceId))
          throw new WorkflowNotFoundException()

        signalValues.updateAndGet { map =>
          if (map.get(key).exists(_ != value))
            throw new SignalConflictException(signal)

          map + (key -> value)
        }
      }
    }
  }

  override def setSignal[A](
                             signal: Signal[A],
                             value: A,
                             ttl: FiniteDuration
                           )(using workflowCtx: SimpleWorkflowContext): Unit =
    signalStore
      .bind(WorkflowScope(workflowCtx.meta, workflowCtx.instanceId))
      .setSignalValue(signal, value, ttl)
}
