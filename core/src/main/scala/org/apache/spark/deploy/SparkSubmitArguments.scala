package org.apache.spark.deploy

import java.io.{IOException, FileInputStream, File}
import java.util.Properties

import org.apache.spark.SparkException
import org.apache.spark.util.Utils

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2017/5/24.
 */
private[spark] class SparkSubmitArguments(args: Seq[String]) {
  var master: String = null
  var deployMode: String = null
  var executorMemory: String = null
  var executorCores: String = null
  var totalExecutorCores: String = null
  var propertiesFile: String = null
  var driverMemory:String = null
  var driverExtraClassPath:String = null
  var driverExtraLibraryPath:String = null
  var driverExtraJavaOptions: String = null
  var driverCores: String = null
  var supervise:Boolean = false //监督？
  var queue:String = null
  var numExecutors: String = null
  var files:String = null
  var archives:String = null
  var mainClass:String = null
  var primaryResource:String = null
  var name:String = null
  var childArgs:ArrayBuffer[String] = new ArrayBuffer[String]()
  var jars:String = null
  var verbose:Boolean = false
  var isPython:Boolean = false
  var pyFiles:String = null

  parseOpts(args.toList)


  private def parseOpts(opts:Seq[String]): Unit ={
    var inSparkOpts = true
    parse(opts)

    //递归
    def parse(opts:Seq[String]): Unit =opts match {
      case ("--name")::value :: tail =>
        name = value
        parse(tail)
      case ("--master")::value::tail =>
        master = value
        parse(tail)
      case ("--class")::value::tail =>
        mainClass = value
        parse(tail)
      case ("--deploy-mode")::value::tail =>
        if(value != "client" && value != "cluster"){
          SparkSubmit.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
        }
        deployMode = value
        parse(tail)
      case ("-num--executors")::value::tail =>
        numExecutors = value
        parse(tail)
      case ("--total-executor-cores")::value::tail =>
        totalExecutorCores = value
        parse(tail)
      case ("--executor-cores")::value::tail =>
        executorCores = value
        parse(tail)
      case ("--executor-memory")::value::tail =>
        executorMemory = value
        parse(tail)
      case ("--driver-memory")::value::tail =>
        driverMemory = value
        parse(tail)
      case ("--driver-cores")::value::tail =>
        driverCores = value
        parse(tail)
      case ("--driver-class-path")::value::tail =>
        driverExtraClassPath = value
        parse(tail)
      case ("--driver-java-option")::value::tail =>
        driverExtraJavaOptions = value
        parse(tail)
      case ("--driver-library-path")::value::tail =>
        driverExtraLibraryPath = value
        parse(tail)
      case ("--properties-file")::value::tail =>
        propertiesFile = value
        parse(tail)
      case ("--supervise")::tail =>
        supervise = true
        parse(tail)
      case ("-queue")::value::tail =>
        queue = value
        parse(tail)
      case ("--files")::value::tail =>
        files = Utils.resolveURIs(value)
        parse(tail)
      case ("--py-files")::value::tail =>
        pyFiles = Utils.resolveURIs(value)
        parse(tail)
      case ("--archives")::value::tail =>
        archives = Utils.resolveURIs(value)
        parse(tail)
      case ("--jars")::value::tail =>
        jars = Utils.resolveURIs(value)
        parse(tail)
      case ("--help" | "-h")::tail =>
        printUsageAndExit(0)
      case ("--verbose" | "-v")::tail =>
        verbose = true
        parse(tail)
      case value::tail =>
        if(inSparkOpts){
          value match {
            case v if v.startsWith("--") && v.contains("=") && v.split("=").size == 2 =>
              val parts = v.split("=")
              parse(Seq(parts(0), parts(1)) ++ tail)
            case v if v.startsWith("-") =>
              val errorMessage = s"ubrecognized option '$value'."
              SparkSubmit.printErrorAndExit(errorMessage)
            case v =>
              primaryResource =
                if(!SparkSubmit.isShell(v)){
                  Utils.resolveURI(v).toString
                }else{
                  v
                }
              inSparkOpts = false
              isPython = SparkSubmit.isPython(v)
              parse(tail)
          }
        }else{
          if(!value.isEmpty){
            childArgs += value
          }
          parse(tail)
        }
      case Nil =>
    }
  }

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit ={
    val outStream = SparkSubmit.printStream
    if(unknownParam != null){
      outStream.println("Unknown/ussupported param " + unknownParam)
    }
    outStream.println(
    """Usage: spark-submit [options] <app jar | python file> [app options]
      |Options:
      |  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
      |  --deploy-mode DEPLOY_MODE   Where to run the driver program: either "client" to run
      |                              on the local machine, or "cluster" to run inside cluster.
      |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
      |  --name NAME                 A name of your application.
      |  --jars JARS                 Comma-separated list of local jars to include on the driver
      |                              and executor classpaths.
      |  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
      |                              on the PYTHONPATH for Python apps.
      |  --files FILES               Comma-separated list of files to be placed in the working
      |                              directory of each executor.
      |  --properties-file FILE      Path to a file from which to load extra properties. If not
      |                              specified, this will look for conf/spark-defaults.conf.
      |
      |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 512M).
      |  --driver-java-options       Extra Java options to pass to the driver.
      |  --driver-library-path       Extra library path entries to pass to the driver.
      |  --driver-class-path         Extra class path entries to pass to the driver. Note that
      |                              jars added with --jars are automatically included in the
      |                              classpath.
      |
      |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).
      |
      | Spark standalone with cluster deploy mode only:
      |  --driver-cores NUM          Cores for driver (Default: 1).
      |  --supervise                 If given, restarts the driver on failure.
      |
      | Spark standalone and Mesos only:
      |  --total-executor-cores NUM  Total cores for all executors.
      |
      | YARN-only:
      |  --executor-cores NUM        Number of cores per executor (Default: 1).
      |  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
      |  --num-executors NUM         Number of executors to launch (Default: 2).
      |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
      |                              working directory of each executor.
    """.stripMargin
    )
    SparkSubmit.exitFn()
  }

}

object SparkSubmitArguments {

  /** Load properties present in the given file. */
  def getPropertiesFromFile(file:File):Seq[(String, String)] = {
    require(file.exists, s"Properties file ${file.getName} dose not exist")
    val inputStream = new FileInputStream(file)
    val properties = new Properties()
    try {
      properties.load(inputStream)
    } catch {
      case e: IOException =>
        val message = s"Failed when loading Spark properties file ${file.getName}"
        throw new SparkException(message, e)
    }
    properties.stringPropertyNames().toSeq.map(k => (k, properties(k).trim))
  }
}