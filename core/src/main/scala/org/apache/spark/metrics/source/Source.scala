package org.apache.spark.metrics.source

import com.codahale.metrics.MetricRegistry

/**
 * Created by Administrator on 2017/6/2.
 */
private[spark] trait Source {

  def sourceName:String
  def metricRegistry:MetricRegistry

}
