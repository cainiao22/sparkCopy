package org.apache.spark.executor

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * Created by Administrator on 2017/6/12.
 */
private[spark] object ExecutorUncaughtExceptionHandler
  extends Thread.UncaughtExceptionHandler with Logging {

  override def uncaughtException(thread: Thread, exception: Throwable): Unit = {
    try{
      logError("Uncaught exception in thread " + thread, exception)
      if(!Utils.)
    }
  }
}
