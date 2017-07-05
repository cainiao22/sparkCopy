package org.apache.spark.scheduler

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.io.CompressionCodec
import org.apache.spark.util.FileLogger
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.json4s.JsonAST.JValue

import scala.collection.mutable.ArrayBuffer

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
  import EventLoggingListener._
  private val shouldCompress = sparkConf.getBoolean("spark.eventLog.compress", false)
  private val shouldOverwrite = sparkConf.getBoolean("spark.eventLog.overwrite", false)
  private val testing = sparkConf.getBoolean("spark.eventLog.testing", false)
  private val outputBufferSize = sparkConf.getInt("spark.eventLog.buffer.kb", 100) * 1024
  private val logBaseDir = sparkConf.get("spark.eventLog.dir", DEFAULT_LOG_DIR).stripSuffix("/")
  private val name = appName.replaceAll("[:/]", "-").toLowerCase + "-" + System.currentTimeMillis()
  val logDir = logBaseDir + "/" + name

  protected val logger = new FileLogger(logDir, sparkConf, hadoopConf, outputBufferSize,
    shouldCompress, shouldCompress, Some(LOG_FILE_PERMISSIONS))

  private[scheduler] val loggedEvents = new ArrayBuffer[JValue]

  def start(): Unit ={
    //仅仅是创建了一个日志目录
    logger.start()
    logInfo("Logging events to %s".format(logDir))
    if(shouldCompress){
      val codec = sparkConf.get("spark.io.compression.codec", CompressionCodec.DEFAULT_COMPRESSION_CODEC)
      logger.newFile(COMPRESSION_CODEC_PREFIX + codec)

    }
  }


}

private[spark] object EventLoggingListener {
  val DEFAULT_LOG_DIR = "/tmp/spark-events"
  val LOG_PREFIX = "EVENT_LOG_"
  val SPARK_VERSION_PREFIX = "SAPRK_VERSION_"
  val COMPRESSION_CODEC_PREFIX = "COMPRESSION_CODEC_"
  val LOG_FILE_PERMISSIONS = FsPermission.createImmutable(Integer.parseInt("770", 8).toShort)
}
