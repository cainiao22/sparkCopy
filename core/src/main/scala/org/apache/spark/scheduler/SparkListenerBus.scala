package org.apache.spark.scheduler

import org.apache.spark.Logging
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * A SparkListenerEvent bus that relays events to its listeners
 */
private[spark] trait SparkListenerBus extends Logging {

  /**
   * 面向切面编程的一种应用。相当于new了一个SynchronizedBuffer，但是它的父类是ArrayBuffer，以此类推，
   * 类似于适配器或者代理模式
   */
  protected val sparkListeners = new ArrayBuffer[SparkListener]
    with mutable.SynchronizedBuffer[SparkListener]

  def addListener(listener: SparkListener): Unit = {
    sparkListeners += listener
  }

  /**
   * Post an event to all attached listeners.
   * This does nothing if the event is SparkListenerShutdown.
   */
  def postToAll(event: SparkListenerEvent): Unit = {
    //todo 各种事件的处理

  }

  /**
   * Apply the given function to all attached listeners, catching and logging any exception.
   */
  def foreachListener(f: SparkListener => Unit): Unit = {
    sparkListeners.foreach { listener =>
      try {
        f(listener)
      } catch {
        case e: Exception =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

}
