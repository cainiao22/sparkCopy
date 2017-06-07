package org.apache.spark.deploy.master

/**
 * Created by Administrator on 2017/6/7.
 */
sealed trait MasterMessages extends Serializable

private[master] object MasterMessages {

  // LeaderElectionAgent to Master
  case object ElectedLeader

  case object RevokedLeadership


  // Actor System to Master
  case object CheckForWorkerTimeOut

}
