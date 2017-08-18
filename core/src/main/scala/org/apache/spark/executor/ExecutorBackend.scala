package org.apache.spark.executor

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState

/**
 * A pluggable interface used by the Executor to send updates to the cluster scheduler.
 */
private[spark] trait ExecutorBackend {
  def statusUpdate(taskId:Long, state:TaskState, data:ByteBuffer)

  // Exists as a work around for SPARK-1112. This only exists in branch-1.x of Spark.
  def akkaFrameSize(): Long = Long.MaxValue
}
