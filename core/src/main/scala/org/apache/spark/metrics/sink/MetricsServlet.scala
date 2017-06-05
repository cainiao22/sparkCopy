package org.apache.spark.metrics.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.json.MetricsModule
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Created by Administrator on 2017/6/5.
 */
private[spark] class MetricsServlet(val property:Properties, val registry:MetricRegistry,
                                     securityManager: SecurityManager) extends Sink {

  val SERVLET_KEY_PATH = "path"
  val SERVLET_KEY_SAMPLE = "sample"

  val SERVLET_DEFAULT_SAMPLE = false

  val servletPath = property.get(SERVLET_KEY_PATH)
  val servletShowSample = Option(property.getProperty(SERVLET_KEY_SAMPLE).toBoolean)
    .getOrElse(SERVLET_DEFAULT_SAMPLE)

  val mapper = new ObjectMapper().registerModule(
    new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS, servletShowSample))




  override def start: Unit = {}

  override def stop: Unit = {}
}
