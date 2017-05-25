package org.apache.spark.deploy

import java.io.PrintStream

import org.apache.spark.util.Utils

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2017/5/24.
 */
object SparkSubmit {

  private val YARN = 1
  private val STANDALONE = 2
  private val MESOS = 4
  private val LOCAL = 8
  private val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL

  private var clusterManager: Int = LOCAL

  private val SPARK_SHELL = "spark-shell"
  private val PYSPARK_SHELL = "pyspark-shell"

  def main(args: Array[String]): Unit = {
    val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      printStream.println(appArgs)
    }
    val (childArgs, classPath, sysProps, mainClass) = createLaunchEnv(appArgs)
  }

  private[spark] var printStream: PrintStream = System.err
  private[spark] var exitFn: () => Unit = () => System.exit(-1)

  private[spark] def printErrorAndExit(str: String): Unit = {
    printStream.println("Error:" + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn
  }

  /**
   * Return whether the given primary resource represents a shell.
   */
  private[spark] def isShell(primaryResource: String): Boolean = {
    primaryResource == SPARK_SHELL || primaryResource == PYSPARK_SHELL
  }

  /**
   * Return whether the given primary resource requires running python.
   */
  private[spark] def isPython(primaryResource: String): Boolean = {
    primaryResource.endsWith(".py") || primaryResource == PYSPARK_SHELL
  }

  /**
   * Return whether the given primary resource represents a user jar.
   */
  private def isUserJar(primaryResource: String): Boolean = {
    !isShell(primaryResource) && !isPython(primaryResource)
  }

  /**
   * Merge a sequence of comma-separated file lists, some of which may be null to indicate
   * no files, into a single comma-separated string.
   */
  private[spark] def mergeFileLists(lists: String*): String = {
    val merged = lists.filter(_ != null).flatMap(_.split(",")).mkString(",")
    if (merged == "") null else merged
  }

  private[spark] def printWarning(str: String) = printStream.println("Warning: " + str)

  /**
   * @return a tuple containing the arguments for the child, a list of classpath
   *         entries for the child, a list of system properties, a list of env vars
   *         and the main class for the child
   */
  private[spark] def createLaunchEnv(args: SparkSubmitArguments)
  : (ArrayBuffer[String], ArrayBuffer[String], Map[String, String], String) = {
    if (args.master.startsWith("local")) {
      clusterManager = LOCAL
    } else if (args.master.startsWith("yarn")) {
      clusterManager = YARN
    } else if (args.master.startsWith("spark")) {
      clusterManager = STANDALONE
    } else if (args.master.startsWith("mesos")) {
      clusterManager = MESOS
    } else {
      printErrorAndExit("Master must start with yarn, mesos, spark, or local")
    }

    // Because "yarn-cluster" and "yarn-client" encapsulate both the master
    // and deploy mode, we have some logic to infer the master and deploy mode
    // from each other if only one is specified, or exit early if they are at odds(分歧).
    if (args.deployMode == null &&
      (args.master == "yarn-standalone" || args.master == "yarn-cluster")) {
      args.deployMode = "cluster"
    }
    if (args.deployMode == "cluster" && args.master == "yarn-client") {
      printErrorAndExit("Deploy mode \"cluster\" and master \"yarn-client\" are not compatible")
    }
    if (args.deployMode == "client" &&
      (args.master == "yarn-standalone" || args.master == "yarn-cluster")) {
      printErrorAndExit("Deploy mode \"client\" and master \"" + args.master
        + "\" are not compatible")
    }
    if (args.deployMode == "cluster" && args.master.startsWith("yarn")) {
      args.master = "yarn-cluster"
    }
    if (args.deployMode != "cluster" && args.master.startsWith("yarn")) {
      args.master = "yarn-client"
    }

    var deployOnCluster = Option(args.deployMode).getOrElse("client") == "cluster"

    val childClassPath = new ArrayBuffer[String]()
    val childArgs = new ArrayBuffer[String]()
    val sysProps = new HashMap[String, String]()
    var childMainClass = ""

    val isPython = args.isPython
    val isYarnCluster = clusterManager == YARN && deployOnCluster

    if (clusterManager == MESOS && deployOnCluster) {
      printErrorAndExit("Cannot currently run driver on the cluster in Mesos")
    }

    if (isPython) {
      if (deployOnCluster) {
        printErrorAndExit("Cannot currently run Python driver programs on cluster")
      }
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = "py4j.GatewayServer"
        args.childArgs = ArrayBuffer("--die-on-broken-pipe", "0")
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(args.primaryResource, args.pyFiles) ++ args.childArgs
        args.files = mergeFileLists(args.files, args.primaryResource)
      }
      args.files = mergeFileLists(args.files, args.pyFiles)
      sysProps("spark.submit.pyFiles") = PythonRunner.formatPaths(args.pyFiles).mkString(",")
    }

    if (!deployOnCluster) {
      childMainClass = args.mainClass
      if (isUserJar(args.primaryResource)) {
        childClassPath += args.primaryResource
      }
    } else if (clusterManager == YARN) {
      childMainClass = "org.apache.spark.deploy.yarn.Client"
      childArgs +=("--jar", args.primaryResource)
      childArgs +=("--jar", args.mainClass)
    }

    if (clusterManager == YARN) {
      if (!Utils.classIsLoadable("org.apache.spark.deploy.yarn.Client") && !Utils.isTesting) {
        printErrorAndExit("Could not load YARN classes. " +
          "This copy of Spark may not have been compiled with YARN support.")
      }
    }

    // Special flag to avoid deprecation warnings at the client
    sysProps("SPARK_SUBMIT") = "true"
    val options = List[OptionAssigner]
  }
}

case class OptionAssigner(
                           value: String,
                           clusterManager: Int,
                           deployOnCluster: Boolean,
                           clOption: String = null,
                           sysProp: String = null
                           )
