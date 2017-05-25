package org.apache.spark.deploy

import java.net.URI

import org.apache.spark.api.python.{RedirectThread, PythonUtils}
import org.apache.spark.util.Utils
import py4j.GatewayServer

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * A main class used by spark-submit to launch Python applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
object PythonRunner {

  def main (args: Array[String]){
    val pythonFile = args(0)
    val pyFiles = args(1)
    val otherArgs = args.slice(2, args.length)//返回指定的元素
    val pythonExec = sys.env.get("PYSPARK_PYTHON").getOrElse("python")

    // Format python file paths before adding them to the PYTHONPATH
    val formattedPythonFile = formatPath(pythonFile)
    val formattedPyFiles = formatPaths(pyFiles)

    // Launch a Py4J gateway server for the process to connect to; this will let it see our
    // Java system properties and such
    val gatewayServer = new GatewayServer(null, 0)
    gatewayServer.start()

    // Build up a PYTHONPATH that includes the Spark assembly JAR (where this class is), the
    // python directories in SPARK_HOME (if set), and any files in the pyFiles argument
    val pathElements = new ArrayBuffer[String]()
    pathElements ++= formattedPyFiles
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    val pythonPath = PythonUtils.mergePythonPaths(pathElements:_*) //todo 啥意思？

    val builder = new ProcessBuilder(Seq(pythonExec, "-u", formattedPythonFile) ++ otherArgs)
    val env = builder.environment()
    env.put("PYTHONPATH", pythonPath)
    env.put("PYSPARK_GATEWAY_PORT", "" + gatewayServer.getListeningPort)

    builder.redirectErrorStream(true)// Ugly but needed for stdout and stderr to synchronize
    val process = builder.start()
    new RedirectThread(process.getInputStream, System.out, "redirect output").start()
    System.exit(process.waitFor())
  }

  /**
   * Format the python file path so that it can be added to the PYTHONPATH correctly.
   *
   * Python does not understand URI schemes in paths. Before adding python files to the
   * PYTHONPATH, we need to extract the path from the URI. This is safe to do because we
   * currently only support local python files.
   */
  def formatPath(path:String, testWindows:Boolean = false):String = {
    if(Utils.nonLocalPaths(path, testWindows).nonEmpty){
      throw new IllegalArgumentException("Launching Python applications through " +
        s"spark-submit is currently only supported for local files: $path")
    }
    val windows = Utils.isWindows | testWindows
    var formattedPath = if (windows) Utils.formatWindowsPath(path) else path

    // Strip the URI scheme from the path
    formattedPath =
      new URI(formattedPath).getScheme match {
        case Utils.windowsDriver(d) if (windows) => formattedPath
        case null => formattedPath
        case _ => new URI(formattedPath).getPath
      }

    // Guard against malformed paths potentially throwing NPE
    if (formattedPath == null) {
      throw new IllegalArgumentException(s"Python file path is malformed: $path")
    }

    // In Windows, the drive should not be prefixed with "/"
    // For instance, python does not understand "/C:/path/to/sheep.py"
    formattedPath = if (windows) formattedPath.stripPrefix("/") else formattedPath
    formattedPath
  }

  /**
   * Format each python file path in the comma-delimited list of paths, so it can be
   * added to the PYTHONPATH correctly.
   */
  def formatPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    Option(paths).getOrElse("")
      .split(",").filter(_.nonEmpty)
      .map(p => formatPath(p, testWindows))
  }
}
