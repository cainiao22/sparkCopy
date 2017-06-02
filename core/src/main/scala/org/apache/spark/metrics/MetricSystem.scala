package org.apache.spark.metrics

import org.apache.spark.{Logging, SparkConf}

/**
 * Created by Administrator on 2017/6/2.
 */
private[spakr] class MetricSystem private (val instance:String,
                                            conf:SparkConf,
                                            securityMgr:SecurityManager) extends Logging {

  val confFile = conf.get("spark.metrics.conf", null)


}
