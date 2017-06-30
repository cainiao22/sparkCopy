package org.apache.spark.util

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.MetadataCleanerType.MetadataCleanerType

/**
 * Runs a timer task to periodically clean up metadata (e.g. old files or hashtable entries)
 */
private[spark] class MetadataCleaner(cleanerType: MetadataCleanerType,
                                     cleanFunc: (Long) => Unit,
                                     conf: SparkConf) extends Logging {
  val name = cleanerType.toString

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

}
