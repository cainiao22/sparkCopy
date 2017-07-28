package org.apache.spark.scheduler.cluster

/**
 * Created by QDHL on 2017/7/24.
 */
private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable


private[spark] object CoarseGrainedClusterMessage {

  // Internal messages in driver
  case object ReviveOffers extends CoarseGrainedClusterMessage

}
