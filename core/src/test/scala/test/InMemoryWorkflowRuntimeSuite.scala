package test

import atomicflow.WorkflowRuntime
import atomicflow.impl.memory.InMemoryWorkflowRuntime

class InMemoryWorkflowRuntimeSuite extends WorkflowRuntimeSuite {
  override def createWorkflowRuntime: WorkflowRuntime = InMemoryWorkflowRuntime()

  override def createWorkflowRuntime(clock: TestClock, retryBackoff: scala.concurrent.duration.FiniteDuration): WorkflowRuntime =
    InMemoryWorkflowRuntime(retryBackoff, clock)
}
