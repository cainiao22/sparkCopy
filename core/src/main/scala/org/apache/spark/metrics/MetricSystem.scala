package org.apache.spark.metrics

import java.lang.Iterable
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}
import org.apache.commons.configuration.SubsetConfiguration
import org.apache.hadoop.metrics2.{MetricsTag, MetricsFilter}
import org.apache.spark.metrics.sink.{MetricsServlet, Sink}
import org.apache.spark.metrics.source.Source
import org.apache.spark.{Logging, SparkConf}

import scala.collection.mutable

/**
 * Spark Metrics System, created by specific "instance", combined by source,
 * sink, periodically poll source metrics data to sink destinations.
 *
 * "instance" specify "who" (the role) use metrics system. In spark there are several roles
 * like master, worker, executor, client driver, these roles will create metrics system
 * for monitoring. So instance represents these roles. Currently in Spark, several instances
 * have already implemented: master, worker, executor, driver, applications.
 *
 * "source" specify "where" (source) to collect metrics data. In metrics system, there exists
 * two kinds of source:
 *   1. Spark internal source, like MasterSource, WorkerSource, etc, which will collect
 *   Spark component's internal state, these sources are related to instance and will be
 *   added after specific metrics system is created.
 *   2. Common source, like JvmSource, which will collect low level state, is configured by
 *   configuration and loaded through reflection.
 *
 * "sink" specify "where" (destination) to output metrics data to. Several sinks can be
 * coexisted and flush metrics to all these sinks.
 *
 * Metrics configuration format is like below:
 * [instance].[sink|source].[name].[options] = xxxx
 *
 * [instance] can be "master", "worker", "executor", "driver", "applications" which means only
 * the specified instance has this property.
 * wild card "*" can be used to replace instance name, which means all the instances will have
 * this property.
 *
 * [sink|source] means this property belongs to source or sink. This field can only be
 * source or sink.
 *
 * [name] specify the name of sink or source, it is custom defined.
 *
 * [options] is the specific property of this source or sink.
 */
private[spakr] class MetricSystem private (val instance:String,
                                            conf:SparkConf,
                                            securityMgr:SecurityManager) extends Logging {

  val confFile = conf.get("spark.metrics.conf", null)
  val metricsConfig = new MetricsConfig(Option(confFile))

  val sinks = new mutable.ArrayBuffer[Sink]
  val sources = new mutable.ArrayBuffer[Source]
  val registry = new MetricRegistry

  // Treat MetricsServlet as a special sink as it should be exposed to add handlers to web ui
  private var metricsServlet:Option[MetricsServlet] = None

  //todo getServletHandlers


  metricsConfig.initialize()
  registerSources()
  registerSinks()

  def start(): Unit ={
    sinks.foreach(_.start)
  }

  def stop(): Unit ={
    sinks.foreach(_.stop)
  }


  def registerSources(): Unit ={
    val instConfig = metricsConfig.getInstance(instance)
    val sourceConfigs = metricsConfig.subProperties(instConfig, MetricSystem.SOURCE_REGEX)
    sourceConfigs.foreach{ kv =>
      val classPath = kv._2.getProperty("class")
      try{
        val instance = Class.forName(classPath).newInstance()
        registerSource(instance.asInstanceOf[Source])
      }catch {
        case e: Exception => logError("Source class " + classPath + " cannot be instantialized", e)
      }
    }
  }

  def removeSource(source: Source): Unit ={
    sources -= source
    registry.removeMatching(new MetricFilter(){
       override def matches(name: String, metric: Metric):Boolean = {
         name.startsWith(source.sourceName)
       }
    })
  }

  def registerSource(source: Source): Unit ={
    sources += source
    try{
      registry.register(source.sourceName, source.metricRegistry)
    }catch {
      case e:IllegalArgumentException => logInfo("Metrics already registered", e)
    }
  }

  def registerSinks(): Unit ={
    val instConfig = metricsConfig.getInstance(instance)
    val sinkConfigs = metricsConfig.subProperties(instConfig, MetricSystem.SINK_REGEX)
    sinkConfigs.foreach{kv =>
      val classPath = kv._2.getProperty("class")
      try{
        //这里的构造方式做了一个统一
        val sink = Class.forName(classPath).getConstructor(classOf[Properties],
          classOf[MetricRegistry], classOf[SecurityManager])
          .newInstance(kv._2, registry, securityMgr)
        if(kv._1 == "servlet"){
          metricsServlet = Some(sink.asInstanceOf[MetricsServlet])
        }else{
          sinks += sink.asInstanceOf[Sink]
        }
      }catch {
        case e: Exception => logError("Sink class " + classPath + " cannot be instantialized", e)
      }
    }
  }

}

private[spark] object MetricSystem {
  //^表示匹配开始的位置
  val SINK_REGEX = "^sink\\.(.+)\\.(.+)".r
  val SOURCE_REGEX = "^source\\.(.+)\\.(.+)".r

  val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  val MINIMAL_POLL_PERIOD = 1
}
