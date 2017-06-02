package org.apache.spark.metrics.sink

/**
 * Created by Administrator on 2017/6/2.
 */
private[spark] trait Sink {

  def start: Unit

  def stop: Unit
}
