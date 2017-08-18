package org.apache.spark.executor

import java.nio.ByteBuffer

import org.apache.spark.{SparkEnv, SparkConf, Logging}
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
 * Spark executor used with Mesos, YARN, and the standalone scheduler.
 */
private[spark] class Executor(
                               executorId: String,
                               slaveHostname: String,
                               properties: Seq[(String, String)],
                               isLocal: Boolean = false
                               ) extends Logging {

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
  private val currentJars: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  Utils.checkHost(slaveHostname,"Expected executed slave to be a hostname")
  // must not have port specified.
  assert (0 == Utils.parseHostPort(slaveHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(slaveHostname)

  val conf = new SparkConf()
  conf.setAll(properties)

  // If we are in yarn mode, systems can have different disk layouts so we must set it
  // to what Yarn on this system said was available. This will be used later when SparkEnv
  // created.
  if(java.lang.Boolean.valueOf(
    System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE"))
  )){
    conf.set("spark.local.dir", getYarnLocalDirs())
  }else if(sys.env.contains("SPARK_LOCAL_DIRS")){
    conf.set("spark.local.dir", sys.env("SPARK_LOCAL_DIRS"))
  }

  if(!isLocal){
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(ExecutorUncaughtExceptionHandler)
  }

  val executorSource = new ExecutorSource(this, executorId)

  // Initialize Spark environment (using system properties read above)
  private val env = {
    if(!isLocal){
      val _env = SparkEnv.create
    }
  }

  /** Get the Yarn approved local directories. */
  private def getYarnLocalDirs(): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(System.getenv("YARM_LOCAL_DIRS"))
      .getOrElse(Option(System.getenv("LOCAL_DIRS"))
      .getOrElse(""))

    if (localDirs.isEmpty) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }
}
