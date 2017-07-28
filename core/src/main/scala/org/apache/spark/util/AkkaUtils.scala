package org.apache.spark.util

import java.util.concurrent.TimeUnit

import org.apache.spark.{SparkConf, Logging}

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Various utility classes for working with Akka.
 */
private[spark] object AkkaUtils extends Logging {


  /** Returns the default Spark timeout to use for Akka ask operations. */
  def askTimeout(conf:SparkConf):FiniteDuration = {
    Duration.create(conf.getLong("spark.akka.askTimeout", 30), TimeUnit.SECONDS)
  }


  /** Returns the configured max frame size for Akka messages in bytes. */
  def maxFrameSizeBytes(conf: SparkConf): Int = {
    conf.getInt("spark.akka.frameSize", 10) * 1024 * 1024
  }
}
