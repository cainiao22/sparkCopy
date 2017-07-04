package org.apache.spark.scheduler

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * A SparkListener that logs events to persistent storage.
 *
 * Event logging is specified by the following configurable parameters:
 *   spark.eventLog.enabled - Whether event logging is enabled.
 *   spark.eventLog.compress - Whether to compress logged events
 *   spark.eventLog.overwrite - Whether to overwrite any existing files.
 *   spark.eventLog.dir - Path to the directory in which events are logged.
 *   spark.eventLog.buffer.kb - Buffer size to use when writing to output streams
 */
private[spark] class EventLoggingListener(
                                         appName:String,
                                         sparkConf:SparkConf,
                                         hadoopConf:Configuration = SparkHadoopUtil.get.newConfiguration()
                                           ) extends SparkListener with Logging {



}
