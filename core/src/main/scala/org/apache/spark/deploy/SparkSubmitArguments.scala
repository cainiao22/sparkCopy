package org.apache.spark.deploy

/**
 * Created by Administrator on 2017/5/24.
 */
private[spark] class SparkSubmitArguments(args: Seq[String]) {
  var master:String = null
  var deployMode:String = null
  var executorMemory:String = null
  var executorCores:String = null
}
