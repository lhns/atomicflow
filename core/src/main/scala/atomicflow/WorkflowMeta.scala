package atomicflow

/** @param version the version of the workflow's code. It is stored with every instance at creation, so the body can
  *                branch on `Workflow.versionAtCreation` to keep instances created by older code on their old path. */
case class WorkflowMeta(
                         id: WorkflowId,
                         name: String,
                         description: Option[String],
                         version: Int = 1
                       )
