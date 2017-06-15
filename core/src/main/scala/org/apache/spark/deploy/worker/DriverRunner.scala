package org.apache.spark.deploy.worker

import java.io.{IOException, File}

import org.apache.spark.deploy.master.DriverState

import scala.collection.JavaConversions._

import akka.actor.ActorRef
import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.Logging
import org.apache.spark.deploy.{DriverStateChanged, Command, DriverDescription}
import org.apache.spark.deploy.master.DriverState.DriverState

import scala.collection.Map

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 */
private[spark] class DriverRunner(
                                   val driverId: String,
                                   val workDir: File,
                                   val sparkHome: File,
                                   val driverDesc: DriverDescription,
                                   val worker: ActorRef,
                                   val workerUrl: String //worker的akka地址
                                   ) extends Logging {

  @volatile var process: Option[Process] = None
  @volatile var killed: Boolean = false

  // Populated once finished
  var finalState: Option[DriverState] = None
  var finalException: Option[Exception] = None
  var finalExitCode: Option[Int] = None

  private[deploy] def setClock(_clock: Clock) = clock = _clock

  private[deploy] def setSleeper(_sleeper: Sleeper) = sleeper = _sleeper

  private var clock: Clock = new Clock {
    def currentTimeMillis: Long = System.currentTimeMillis()
  }
  private var sleeper: Sleeper = new Sleeper {
    override def sleep(seconds: Int): Unit = (0 until seconds).takeWhile(p => {
      Thread.sleep(1000); !killed
    })
  }

  /** Starts a thread to run and manage the driver. */
  def start() = {
    new Thread("driverrunner for " + driverId) {
      override def run(): Unit = {
        try {
          val driverDir = createWorkingDirectory()
          val localJarFilename = downloadUserJar(driverDir)

          // Make sure user application jar is on the classpath
          val classPath = driverDesc.command.classPathEntries ++ Seq(localJarFilename)
          val newCommand = Command(
            driverDesc.command.mainClass,
            driverDesc.command.arguments.map(substituteVariables),
            driverDesc.command.environment,
            classPath,
            driverDesc.command.libraryPathEntries,
            driverDesc.command.extraJavaOptions
          )

          val command = CommandUtils.buildCommandSeq(newCommand, driverDesc.mem,
          sparkHome.getAbsolutePath)

          launchDriver(command, driverDesc.command.environment, driverDir, driverDesc.supervise)
        }catch {
          case e:Exception => finalException = Some(e)
        }

        val state =
          if(killed){
            DriverState.KILLED
          }else if(finalException.isDefined){
            DriverState.ERROR
          }else {
            finalState match {
              case 0 => DriverState.FINISHED
              case _ => DriverState.FAILED
            }
          }

        finalState = Some(state)
        worker ! DriverStateChanged(driverId, state, finalException)
      }
    }.start()
  }

  /**
   * Creates the working directory for this driver.
   * Will throw an exception if there are errors preparing the directory.
   */
  private def createWorkingDirectory(): File = {
    val driverDir = new File(workDir, driverId)
    if (!driverDir.exists() && !driverDir.mkdirs()) {
      throw new IOException("Failed to create directory " + driverDir)
    }
    driverDir
  }

  /**
   * Download the user jar into the supplied directory and return its local path.
   * Will throw an exception if there are errors downloading the jar.
   */
  private def downloadUserJar(driverDir: File): String = {
    val jarPath = new Path(driverDesc.jarUrl)
    val emptyConf = new Configuration()
    val jarFileSystem = jarPath.getFileSystem(emptyConf)

    val destPath = new File(driverDir.getAbsolutePath, jarPath.getName)
    val jarFileName = jarPath.getName
    val localJarFile = new File(driverDir, jarFileName)
    val localJarFileName = localJarFile.getAbsolutePath

    if (!localJarFile.exists()) {
      // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar $jarPath to $destPath")
      FileUtil.copy(jarFileSystem, jarPath, destPath, false, emptyConf)
    }

    if (!localJarFile.exists()) {
      // Verify copy succeeded
      throw new Exception(s"Did not see expected jar $jarFileName in $driverDir")
    }

    localJarFileName
  }

  private def launchDriver(command: Seq[String], envVars: Map[String, String], baseDir: File,
                           supervise: Boolean) = {
    val builder = new ProcessBuilder(command:_*).directory(baseDir)
    envVars.map{case (k, v) => builder.environment().put(k, v)}

    def initialize(process: Process): Unit ={
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val header =  "Launch Command: %s\n%s\n\n".format(
        //666 将每个命令用“ ”分割                                 //40个"="
        command.mkString("\"", "\" \"", "\""), "=" * 40)
      Files.append(header, stderr, Charsets.UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr
      )
    }

    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }

  private[deploy] def runCommandWithRetry(command:ProcessBuilderLike, initialize:Process => Unit,
                                          supervise:Boolean): Unit ={
    // Time to wait between submission retries.
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    val successfulRunDuration = 5

    var keepTring = !killed

    while(keepTring){
      logInfo("Launch Command: " + command.command.mkString("\"", "\" \"", "\""))
      synchronized{
        if(killed){return }
        process = Some(command.start())
      }

      val processStart = clock.currentTimeMillis()
      val exitCode = process.get.waitFor()
      if(clock.currentTimeMillis() - processStart >= successfulRunDuration * 1000){
        waitSeconds = 1
      }
      if(supervise && exitCode != 0 && !killed){
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        sleeper.sleep(waitSeconds)
        waitSeconds *= 2
      }

      keepTring = supervise && exitCode != 0 && !killed
      finalExitCode = Some(exitCode)
    }

  }

  /** Replace variables in a command argument passed to us */
  private def substituteVariables(arguments: String): String = arguments match {
    case "{{WORKER_URL}}" => workerUrl
    case other => other
  }

}

private[deploy] trait Clock {
  def currentTimeMillis(): Long
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int)
}


private[deploy] trait ProcessBuilderLike {
  def start():Process
  def command:Seq[String]
}

private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder) = new ProcessBuilderLike {
    override def start(): Process = processBuilder.start()
    override def command = processBuilder.command()
  }
}