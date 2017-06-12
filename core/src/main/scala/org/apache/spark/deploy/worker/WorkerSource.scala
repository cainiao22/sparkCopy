package org.apache.spark.deploy.worker

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.spark.metrics.source.Source

/**
 * Created by Administrator on 2017/6/12.
 */
private[spark] class WorkerSource(val worker:Worker) extends Source {

  val sourceName = "worker"
  val metricRegistry = new MetricRegistry

  metricRegistry.register(MetricRegistry.name("executors"), new Gauge[Int] {
    override def getValue:Int = worker.executors.size
  })

  metricRegistry.register(MetricRegistry.name("coresUsed"), new Gauge[Int] {
    override def getValue:Int = worker.coresUsed
  })

  metricRegistry.register(MetricRegistry.name("memUsed_MB"), new Gauge[Int] {
    override def getValue:Int = worker.memoryUsed
  })

  metricRegistry.register(MetricRegistry.name("coresFree"), new Gauge[Int] {
    override def getValue:Int = worker.coresFree
  })

  metricRegistry.register(MetricRegistry.name("memUsed_MB"), new Gauge[Int] {
    override def getValue:Int = worker.memoryFree
  })

}
