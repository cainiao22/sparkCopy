package org.apache.spark

/**
 * Created by Administrator on 2017/5/24.
 */
class SparkException(message:String, cause:Throwable) extends Exception(message, cause) {

  def this(message:String) = this(message, null)
}
