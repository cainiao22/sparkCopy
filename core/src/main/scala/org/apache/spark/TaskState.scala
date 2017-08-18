package org.apache.spark

/**
 * Created by QDHL on 2017/8/8.
 */
private[spark] object TaskState extends Enumeration {

  val LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST = Value

  val FINISHED_STATES = Set(FINISHED, FAILED, KILLED, LOST)

  type TaskState = Value

  def isFinished(state: TaskState) = FINISHED_STATES.contains(state)
}
