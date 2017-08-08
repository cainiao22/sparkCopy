package org.apache.spark.scheduler

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import org.apache.spark.serializer.SerializerInstance

import scala.collection.mutable.HashMap

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 * - [[org.apache.spark.scheduler.ShuffleMapTask]]
 * - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 */
private[spark] abstract class Task[T](var stageId: Int, var partitionId: Int) extends Serializable {

  // Map output tracker epoch. Will be set by TaskScheduler.
  var epoch: Long = -1

  def preferredLocations: Seq[TaskLocation] = Nil
}

/**
 * Handles transmission of tasks and their dependencies, because this can be slightly tricky. We
 * need to send the list of JARs and files added to the SparkContext with each task to ensure that
 * worker nodes find out about it, but we can't make it part of the Task because the user's code in
 * the task might depend on one of the JARs. Thus we serialize each task as multiple objects, by
 * first writing out its dependencies.
 */
private[spark] object Task {
  /**
   * Serialize a task and the current app dependencies (files and JARs added to the SparkContext)
   */
  def serializeWithDependencies(
                                 task: Task[_],
                                 currentFiles: HashMap[String, Long],
                                 currentJars: HashMap[String, Long],
                                 serializer: SerializerInstance)
  : ByteBuffer = {
    val out = new ByteArrayOutputStream(4096)
    val dataOut = new DataOutputStream(out)

    //write current files
    dataOut.writeInt(currentFiles.size)
    for((name, timestamp) <- currentFiles){
      dataOut.writeUTF(name)
      dataOut.writeLong(timestamp)
    }

    //write current jars
    dataOut.writeInt(currentJars.size)
    for((jar, timestamp) <- currentJars){
      dataOut.writeUTF(jar)
      dataOut.writeLong(timestamp)
    }

    // Write the task itself and finish
    dataOut.flush()
    val taskBytes = serializer.serialize(task).array()
    out.write(taskBytes)
    //将数组包装成ByteBuffer
    ByteBuffer.wrap(out.toByteArray)
  }
}
