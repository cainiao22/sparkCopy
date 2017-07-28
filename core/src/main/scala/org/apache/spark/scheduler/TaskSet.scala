package org.apache.spark.scheduler

import java.util.Properties

/**
 * A set of tasks submitted together to the low-level TaskScheduler, usually representing
 * missing partitions of a particular stage.
 */
private[spark] class TaskSet(val tasks:Array[Task[_]],
                              val stageId:Int,
                              val attempt:Int,
                              val priority:Int,
                              val properties: Properties) {

}
