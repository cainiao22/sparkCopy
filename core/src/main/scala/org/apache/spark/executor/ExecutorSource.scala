package org.apache.spark.executor

import com.codahale.metrics.MetricRegistry
import org.apache.spark.metrics.source.Source

/**
 * Created by QDHL on 2017/8/17.
 */
private[spark] class ExecutorSource(val executor: Executor, executorId:String) extends Source {
  override def sourceName: String = ???

  override def metricRegistry: MetricRegistry = ???
}
