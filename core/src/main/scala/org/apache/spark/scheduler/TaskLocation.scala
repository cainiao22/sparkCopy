package org.apache.spark.scheduler

/**
 * A location where a task should run. This can either be a host or a (host, executorID) pair.
 * In the latter case, we will prefer to launch the task on that executorID, but our next level
 * of preference will be executors on the same host if this is not possible.
 */
private[spark]
class TaskLocation private (val host:String, val executorId:Option[String])
extends Serializable {
  override def toString: String = "TaskLocation(" + host + ", " + executorId + ")"
}

private[spark] object TaskLocation {
  def apply(host: String, executorId: String) = new TaskLocation(host, Some(executorId))

  def apply(host: String) = new TaskLocation(host, None)
}