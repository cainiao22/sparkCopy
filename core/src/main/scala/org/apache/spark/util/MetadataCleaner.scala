package org.apache.spark.util

import java.util.{TimerTask, Timer}

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.MetadataCleanerType.MetadataCleanerType

/**
 * Runs a timer task to periodically clean up metadata (e.g. old files or hashtable entries)
 */
private[spark] class MetadataCleaner(cleanerType: MetadataCleanerType,
                                     cleanFunc: (Long) => Unit,
                                     conf: SparkConf) extends Logging {
  val name = cleanerType.toString
  private val delaySeconds = MetadataCleaner.getDelaySeconds(conf, cleanerType)
  private val periodSeconds = math.max(delaySeconds / 10, 10)
  private val timer = new Timer(name + " cleanup timer", true)

  private val task = new TimerTask {
    override def run(): Unit = {
      try{
        cleanFunc(System.currentTimeMillis() - delaySeconds*1000)
      }catch {
        case e: Exception => logError("Error running cleanup task for " + name, e)
      }
    }
  }

  if(delaySeconds > 0){
    logDebug(
      "Starting metadata cleaner for " + name + " with delay of " + delaySeconds + " seconds " +
        "and period of " + periodSeconds + " secs")
    timer.schedule(task, periodSeconds * 1000, periodSeconds * 1000)
  }

  def cancel(): Unit ={
    timer.cancel()
  }

}

private[spark] object MetadataCleanerType extends Enumeration {
  val MAP_OUTPUT_TRACKER, SPARK_CONTEXT, HTTP_BROADCAST, BLOCK_MANAGER,
  SHUFFLE_BLOCK_MANAGER, BROADCASR_VARS = Value

  type MetadataCleanerType = Value

  def systemProperty(which: MetadataCleanerType): String = {
    "spark.cleaner.ttl." + which.toString
  }
}

private[spark] object MetadataCleaner {
  def getDelaySeconds(conf: SparkConf): Int = {
    conf.getInt("spark.cleaner.ttl", -1)
  }

  def getDelaySeconds(conf: SparkConf,
                      cleanerType: MetadataCleanerType.MetadataCleanerType): Int = {
    conf.get(MetadataCleanerType.systemProperty(cleanerType), getDelaySeconds(conf).toString).toInt
  }
}
