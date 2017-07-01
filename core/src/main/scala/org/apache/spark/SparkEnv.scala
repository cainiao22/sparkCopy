package org.apache.spark

/**
 * Created by Administrator on 2017/6/26.
 */
class SparkEnv {

}

object SparkEnv extends Logging {

  private val env = new ThreadLocal[SparkEnv]

  @volatile private var lastsetSparkEnv:SparkEnv = _


  /**
   * Returns the ThreadLocal SparkEnv, if non-null. Else returns the SparkEnv
   * previously set in any thread.
   */
  def get():SparkEnv = {
    Option(env.get()).getOrElse(lastsetSparkEnv)
  }

}
