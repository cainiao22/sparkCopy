package org.apache.spark.scheduler

import java.nio.ByteBuffer

/**
 * Created by QDHL on 2017/7/25.
 */
private[spark] class TaskDescription(
                                    val taskId:Long,
                                    val executorId:String,
                                    val name:String,
                                    val index:Int, // Index within this task's TaskSet
                                    val _serializedTask:ByteBuffer
                                      ) extends Serializable {

}
