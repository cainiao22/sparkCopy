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

      // We may have been called from a shutdown hook. If so, we must not call System.exit().
      // (If we do, we will deadlock.)
      if(!Utils.inShutdown()){
        if(exception.isInstanceOf[OutOfMemoryError]){
          System.exit(ExecutorExitCode.OOM)
        }else {
          System.exit(ExecutorExitCode.UNCAUGHT_EXCEPTION)
        }
      }
    }catch {
      //halt强制退出 他不会执行钩子函数和finalizer方法，而是直接退出
      case oom: OutOfMemoryError => Runtime.getRuntime.halt(ExecutorExitCode.OOM)
      case t: Throwable => Runtime.getRuntime.halt(ExecutorExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }

  def uncaughtException(exception: Throwable): Unit ={
    uncaughtException(Thread.currentThread(), exception)
  }
}
