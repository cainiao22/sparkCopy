package org.apache.spark.deploy.master

/**
 * Created by Administrator on 2017/6/6.
 */
private[spark] object RecoveryState extends Enumeration {
  type MasterState = Value
  val STANDBY, ALIVE, RECOVERING, COMPLETING_RECOVERY = Value
}
