package org.apache.spark.deploy.worker

import java.io.{IOException, File}

import akka.actor.ActorRef
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.Logging
import org.apache.spark.deploy.{Command, DriverDescription}
import org.apache.spark.deploy.master.DriverState.DriverState

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 */
private[spark] class DriverRunner(
                                   val driverId: String,
                                   val workDir: File,
                                   val sparkHome: File,
                                   val driverDesc: DriverDescription,
                                   val worker: ActorRef,
                                   val workerUrl: String
                                   ) extends Logging {

  @volatile var process: Process = null
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

          val command = CommandUtils
        }
      }
    }
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
