package org.apache.spark.deploy.worker

import java.io.File

import akka.actor.ActorRef
import org.apache.spark.Logging
import org.apache.spark.deploy.{ExecutorState, ApplicationDescription}

/**
 * Manages the execution of one executor process.
 */
private[spark] class ExecutorRunner(
                                   val appId:String,
                                   val execId:Int,
                                   val appDesc:ApplicationDescription,
                                   val cores:Int,
                                   val memory:Int,
                                   val worker:ActorRef,
                                   val workerId:String,
                                   val host:String,
                                   val sparkHome:File,
                                   val workDir:File,
                                   val workerUrl:String,
                                   var state:ExecutorState.Value
                                     ) extends Logging {

  val fullId = appId + "/" + execId
  var workerThread:Thread = null
  var process:Process = null

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  var shutdownHook:Thread = null


  def start(): Unit ={
    workerThread = new Thread("ExecutorRunner for " + fullId){
      override def run(){fetchAndRunExecutor()}
    }
    workerThread.start()

    // Shutdown hook that kills actors on shutdown.
    shutdownHook = new Thread(){
      override def run(){}
    }

    Runtime.getRuntime.addShutdownHook(shutdownHook)
  }

  //todo 实现 fetchAndRunExecutor
  def fetchAndRunExecutor(): Unit ={

  }

}
