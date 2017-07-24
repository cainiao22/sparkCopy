package org.apache.spark.scheduler.cluster

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.TaskSchedulerImpl

/**
 * Created by QDHL on 2017/7/18.
 */
private[spark] class SparkDeploySchedulerBackend(scheduler: TaskSchedulerImpl,
                                                 sc: SparkContext,
                                                 masters: Array[String])
  extends CoarseGrainedSchedulerBackend {

}
