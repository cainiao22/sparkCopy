package org.apache.spark.deploy.master

/**
 * Created by Administrator on 2017/6/8.
 */
private[spark] object ApplicationState extends Enumeration {

  type ApplicationState = Value

  val WAITING, RUNNING, FINISHED, FAILED, UNKNOWN = Value

  val MAX_NUM_RETRY = 10

}
