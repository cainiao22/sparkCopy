package org.apache.spark.deploy.master

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.metrics.source.Source

/**
 * Created by Administrator on 2017/6/5.
 */
private[spark] class MasterSource(val master:Master) extends Source {

  override def sourceName: String = "master"

  override def metricRegistry: MetricRegistry = new MetricRegistry

  metricRegistry.register(MetricRegistry.name("workers"), new Gauge[Int] {
    override def getValue:Int = master.workers.size
  })

  metricRegistry.register(MetricRegistry.name("apps"), new Gauge[Int] {
    override def getValue:Int = master.apps.size
  })

  metricRegistry.register(MetricRegistry.name("waitingApps"), new Gauge[Int] {
    override def getValue:Int = master.waitingApps.size
  })
}
