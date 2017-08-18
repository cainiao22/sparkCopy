package org.apache.spark.scheduler.cluster

import org.apache.spark.util.SerializableBuffer

/**
 * Created by QDHL on 2017/7/24.
 */
private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable


private[spark] object CoarseGrainedClusterMessages {

  // Driver to executors
  case class LaunchTask(data: SerializableBuffer) extends CoarseGrainedClusterMessage

  // Internal messages in driver
  case object ReviveOffers extends CoarseGrainedClusterMessage

}
