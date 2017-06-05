package org.apache.spark.deploy.master

import com.codahale.metrics.MetricRegistry
import org.apache.spark.metrics.source.Source

/**
 * Created by Administrator on 2017/6/5.
 */
private[spark] class MasterSource(val master:Master) extends Source {

  override def sourceName: String = "master"

  override def metricRegistry: MetricRegistry = new MetricRegistry

  //todo register app、worker、driver
  //metricRegistry.register(MetricRegistry.name("works"), )
}
