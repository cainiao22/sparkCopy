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
}
