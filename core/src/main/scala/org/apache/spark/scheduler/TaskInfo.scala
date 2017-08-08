package org.apache.spark.scheduler


/**
 * :: DeveloperApi ::
 * Information about a running task attempt inside a TaskSet.
 */
class TaskInfo( val taskId: Long,
                val index: Int,
                val launchTime: Long,
                val executorId: String,
                val host: String,
                val taskLocality: TaskLocality.TaskLocality) {

}
