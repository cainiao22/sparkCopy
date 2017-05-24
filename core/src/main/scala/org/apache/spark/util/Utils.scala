package org.apache.spark.util

import org.apache.spark.Logging

/**
 * Created by Administrator on 2017/5/23.
 */
private[spark] object Utils extends Logging {

  /**
   * Get the ClassLoader which loaded Spark.
   */
  def getSparkClassLoader = getClass.getClassLoader
}
