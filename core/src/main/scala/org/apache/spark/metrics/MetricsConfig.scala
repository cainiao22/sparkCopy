package org.apache.spark.metrics

import java.io.{FileInputStream, InputStream}
import java.util.Properties

import org.apache.spark.Logging
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Created by Administrator on 2017/6/2.
 */
private[spark] class MetricsConfig(val configFile:Option[String]) extends Logging {

  val DEFAULT_PREFIX = "*"
  val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  val METRICS_CONF = "metrics.properties"

  val properties = new Properties()
  var propertyCategories:mutable.HashMap[String, Properties] = null

  private def setDefaultProperties(prop: Properties): Unit ={
    prop.setProperty("*.sink.servlet.class", "org.apache.spark.metrics.sink.MetricsServlet")
    prop.setProperty("*.sink.servlet.path", "/metrics/json")
    prop.setProperty("master.sink.servlet.path", "/metrics/master/json")
    prop.setProperty("applications.sink.servlet.path", "/metrics/applications/json")
  }

  def initialize(): Unit ={
    // Add default properties in case there's no properties file
    setDefaultProperties(properties)

    var is:InputStream = null
    try{
      is = configFile match {
        case Some(f) => new FileInputStream(f)
        case None => Utils.getSparkClassLoader.getResourceAsStream(METRICS_CONF)
      }

      if(is != null){
        properties.load(is)
      }
    }catch {
      case e:Exception => logError("Error loading configure file", e)
    }finally {
      if(is != null) is.close()
    }

    propertyCategories = subProperties(properties, INSTANCE_REGEX)

  }


  def subProperties(prop:Properties, regex:Regex):mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    import scala.collection.JavaConversions._
    prop.foreach{kv =>
      if(regex.findPrefixMatchOf(kv._1).isDefined){
        val regex(prefix, suffix) = kv._1
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2)
      }
    }

    subProperties
  }
}
