package org.apache.spark.deploy.master

/**
 * Created by Administrator on 2017/6/8.
 */
private[spark] object WorkerState extends Enumeration {

  type WorkerState = Value

  //todo DECOMMISSIONED 退役了 这是啥意思？
  val ALIVE, DEAD, DECOMMISSIONED, UNKNOWN = Value

}
