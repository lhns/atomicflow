package test

import atomicflow.*
import atomicflow.Cacheable.Simple.given_Cacheable_String
import atomicflow.StepAuditEvent.{StepCompleted, StepFailed, StepStarted}
import atomicflow.StepAuditOutcome.{Computed, ShortCircuited}
import atomicflow.impl.memory.InMemoryWorkflowRuntime
import munit.FunSuite

import scala.collection.mutable.ArrayBuffer

class StepAuditLoggingSuite extends FunSuite {
  private final class CollectingAuditLogger extends AuditLogger {
    private val buffer = ArrayBuffer.empty[StepAuditEvent]

    override def log(event: StepAuditEvent): Unit = buffer.synchronized {
      buffer += event
    }

    def events: List[StepAuditEvent] = buffer.synchronized(buffer.toList)
  }

  test("audit logger records started and completed events for a computed step") {
    given WorkflowRuntime = new InMemoryWorkflowRuntime()

    val logger = new CollectingAuditLogger

    val workflow = Workflow(WorkflowId("5b3f4ec0-8b12-437d-9445-3f1c2e0be19a"), name = "audit-test") { (input: String) =>
      Step(StepId("94c11769-23b5-4bb9-a2a1-27fd9687d492"), version = 0, name = "uppercase") {
        input.toUpperCase
      }
      ()
    }

    val instance = workflow.instance(WorkflowInstanceId.generate).withAuditLogger(logger)

    instance.run("hello")

    val events = logger.events
    val started = events.collect { case e: StepStarted => e }
    val completions = events.collect { case e: StepCompleted => e }
    val failures = events.collect { case e: StepFailed => e }

    assertEquals(started.length, 1)
    assertEquals(completions.length, 1)
    assertEquals(completions.head.outcome, Computed)
    assertEquals(failures.length, 0)
  }

  test("audit logger marks short circuited outcome when step result is reused") {
    given WorkflowRuntime = new InMemoryWorkflowRuntime()

    val logger = new CollectingAuditLogger

    val stepId = StepId("ea54cb4c-5383-4a82-ac99-6c4439efcce4")

    val workflow = Workflow(WorkflowId("f0656d6b-6efc-4f37-8e6e-8c3f9abaf6c7"), name = "audit-cache-test") { (input: String) =>
      Step(stepId, version = 0, name = "cached-step") {
        Step.cache(StepInput("input", input))
        input.reverse
      }
    }

    val instance = workflow.instance(WorkflowInstanceId.generate).withAuditLogger(logger)

    instance.run("value")
    instance.recover()

    val outcomes = logger.events.collect { case StepCompleted(_, _, _, outcome) => outcome }

    assert(clue(outcomes.contains(ShortCircuited)))
  }
}
