package org.apache.spark.deploy.worker

import java.io.{IOException, File}

import akka.actor.ActorRef
import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.spark.Logging
import org.apache.spark.deploy.DeployMessages.ExecutorStateChanged
import org.apache.spark.deploy.{ExecutorStateChanged, Command, ExecutorState, ApplicationDescription}

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
      override def run(){killProcess()}
    }

    Runtime.getRuntime.addShutdownHook(shutdownHook)
  }

  //todo 实现 fetchAndRunExecutor
  def fetchAndRunExecutor(): Unit ={
    try{
      // Create the executor's working directory
      val executorDir = new File(workDir, appId + "/" + execId)
      if(!executorDir.mkdirs()){
        throw new IOException("Failed to create directory " + executorDir)
      }

      val command = getCommandSeq
      logInfo("Launch command: " + command.mkString("\"", "\" \"", "\""))
      val builder = new ProcessBuilder(command:_*).directory(executorDir)
      val env = builder.environment()
      appDesc.command.environment.map{case(k, v) => env.put(k, v)}

      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      env.put("SPARK_LAUNCH_WITH_SCALA", "0")
      process = builder.start()
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        command.mkString("\"", "\" \"", "\""), "=" * 40)

      // Redirect its stdout and stderr to files
      val stdout = new File(workDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(workDir, "stderr")
      Files.write(header, stderr, Charsets.UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
      // Wait for it to exit; this is actually a bad thing if it happens, because we expect to run
      // long-lived processes only. However, in the future, we might restart the executor a few
      // times on the same machine.
      val exitCode = process.waitFor()
      //因为没有finished状态，作为完成状态，只有这个正常点
      val state = ExecutorState.FAILED
      val message = "Command exited with code " + exitCode
      worker ! ExecutorStateChanged(appId, execId, state, Some(message), None)
    }catch {
      case interrupted:InterruptedException => {
        logInfo("Runner thread for executor " + fullId + " interrupted")
        killProcess()
      }
      case e:Exception => {
        logError("Error running executor", e)
        killProcess()
        state = ExecutorState.FAILED
        val message = e.getClass + ": " + e.getMessage
        worker ! ExecutorStateChanged(appId, execId, state, Some(message), None)
      }
    }
  }

  def killProcess(): Unit ={
    if(process != null){
      logInfo("Killing process!")
      process.destroy()
      process.waitFor()
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  def substituteVariables(argument: String): String = argument match {
    case "{{WORKER_URL}}" => workerUrl
    case "{{EXECUTOR_ID}}" => execId.toString
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case other => other
  }

  def getCommandSeq = {
    val command = Command(appDesc.command.mainClass,
      appDesc.command.arguments.map(substituteVariables) ++ Seq(appId), appDesc.command.environment,
      appDesc.command.classPathEntries, appDesc.command.libraryPathEntries,
      appDesc.command.extraJavaOptions)

    CommandUtils.buildCommandSeq(command, memory, sparkHome.getAbsolutePath)
  }

  def kill(): Unit ={
    if(workerThread != null){
      workerThread.interrupt()
      state = ExecutorState.KILLED
      worker ! ExecutorStateChanged(appId, execId, state, None, None)
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
    }
  }
}
