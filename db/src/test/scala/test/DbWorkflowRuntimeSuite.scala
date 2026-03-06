package test

import atomicflow.*
import atomicflow.impl.db.DbWorkflowRuntime
import atomicflow.impl.db.DbWorkflowRuntime.DbConfig

import scala.concurrent.duration.FiniteDuration

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

    override def createWorkflowInstance[In, Out](
                                                  workflowInstance: WorkflowInstanceBuilder[In, Out],
                                                  in: In
                                                )(
                                                  using Cacheable[In]
                                                ): Unit =
      unavailable

    override def runWorkflowInstance[In, Out](
                                               workflowInstance: WorkflowInstanceBuilder[In, Out],
                                               in: In
                                             )(
                                               using Cacheable[In]
                                             ): Out =
      unavailable

    override def recoverWorkflowInstance[In, Out](
                                                   workflowInstance: WorkflowInstanceBuilder[In, Out]
                                                 )(
                                                   using Cacheable[In]
                                                 ): Out =
      unavailable

    override def setSignal[A](
                               signal: Signal[A],
                               value: A,
                               ttl: FiniteDuration
                             )(using SimpleWorkflowContext): Unit =
      unavailable
  }

  override def createWorkflowRuntime: WorkflowRuntime =
    if (missingDbVars.nonEmpty) UnavailableRuntime
    else DbWorkflowRuntime(dbConfig)
}
