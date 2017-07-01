package org.apache.spark.deploy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.Logging

/**
 * Contains util methods to interact with Hadoop from Spark.
 */
class SparkHadoopUtil extends Logging {
  val conf:Configuration = newConfiguration
  UserGroupInformation.setConfiguration(conf)

  /**
   * Return an appropriate (subclass) of Configuration. Creating config can initializes some Hadoop
   * subsystems.
   */
  def newConfiguration() = new Configuration()
}
