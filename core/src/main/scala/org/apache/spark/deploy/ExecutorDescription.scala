package org.apache.spark.deploy


/**
 * Created by Administrator on 2017/6/13.
 */
private[spark] class ExecutorDescription(
                                        val appId:String,
                                        val execId:Int,
                                        val cores:Int,
                                        val state:ExecutorState.Value
                                          ) extends Serializable {
  override def toString: String =
    "ExecutorState(appId=%s, execId=%d, cores=%d, state=%s)".format(appId, execId, cores, state)
}
