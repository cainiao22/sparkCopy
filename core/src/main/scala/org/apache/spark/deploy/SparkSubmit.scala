package org.apache.spark.deploy

import java.io.PrintStream

import org.apache.spark.util.Utils

import scala.collection.mutable.{HashMap, Map}
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable

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
    launch(childArgs, classPath, sysProps, appArgs.verbose)
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

    val deployOnCluster = Option(args.deployMode).getOrElse("client") == "cluster"

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
    val options = List[OptionAssigner](
      OptionAssigner(args.master, ALL_CLUSTER_MGRS, false, sysProp = "spark.master"),
      OptionAssigner(args.name, ALL_CLUSTER_MGRS, false, sysProp = "spark.app.name"),
      OptionAssigner(args.name, YARN, true, clOption = "--name", sysProp = "spark.app.name"),
      OptionAssigner(args.driverExtraClassPath, STANDALONE | YARN, true,
        sysProp = "spark.driver.extraClassPath"),
      OptionAssigner(args.driverExtraJavaOptions, STANDALONE | YARN, true,
          sysProp = "spark.driver.extraJavaOptions"),
      OptionAssigner(args.driverExtraLibraryPath, STANDALONE | YARN, true,
          sysProp = "spark.driver.driverExtraLibraryPath"),
      OptionAssigner(args.driverMemory, YARN, true, clOption = "--driver-memory"),
      OptionAssigner(args.driverMemory, STANDALONE, true, clOption = "--memory"),
      OptionAssigner(args.driverCores, STANDALONE, true, clOption = "--cores"),
      OptionAssigner(args.queue, YARN, true, clOption = "--queue"),
      OptionAssigner(args.queue, YARN, false, sysProp = "spark.yarn.queue"),
      OptionAssigner(args.numExecutors, YARN, true, clOption = "--num-executors"),
      OptionAssigner(args.numExecutors, YARN, false, sysProp = "spark.yarn.instances"),
      OptionAssigner(args.totalExecutorCores, STANDALONE | MESOS, false, sysProp = "spark.cores.max"),
      OptionAssigner(args.files, YARN, false, "spark.yarn.dist.files"),
      OptionAssigner(args.files, YARN, true, clOption = "--files"),
      OptionAssigner(args.files, LOCAL | STANDALONE | MESOS, false, sysProp = "spark.files"),
      OptionAssigner(args.files, LOCAL | STANDALONE | MESOS, true, sysProp = "spark.files"),
      OptionAssigner(args.archives, YARN, false, sysProp = "spark.yarn.dist.archives"),
      OptionAssigner(args.archives, YARN, true, clOption = "--archives"),
      OptionAssigner(args.jars, YARN, true, clOption = "--addJars"),
      OptionAssigner(args.jars, ALL_CLUSTER_MGRS, false, sysProp = "spark.jars")
    )

    // For client mode make any added jars immediately visible on the classpath
    if(args.jars != null && deployOnCluster){
      for(jar <- args.jars.split(",")){
        childClassPath += jar
      }
    }

    // Map all arguments to command-line options or system properties for our chosen mode
    for(opt <- options){
      if(opt.value != null && deployOnCluster == opt.deployOnCluster &&
        (clusterManager & opt.clusterManager) != 0){
        if(opt.clOption != null){
          childArgs += (opt.clOption, opt.value)
        }
        if(opt.sysProp != null){
          sysProps.put(opt.sysProp, opt.value)
        }
      }
    }

    // Add the application jar automatically so the user doesn't have to call sc.addJar
    // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
    // For python files, the primary resource is already distributed as a regular file
    if(!isYarnCluster && !isPython) {
      var jars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq())
      if(isUserJar(args.primaryResource)){
        jars += args.primaryResource
      }
      sysProps.put("spark.jars", jars.mkString(","))
    }

    // Standalone cluster specific configurations
    if(deployOnCluster && clusterManager == STANDALONE){
      if(args.verbose){
        childArgs += "--supervise"
      }

      childMainClass = "org.apache.spark.deploy.Client"
      childArgs += "launch"
      childArgs += (args.master, args.primaryResource, args.mainClass)
    }

    // Read from default spark properties, if any
    if(args.childArgs != null){
      if(!deployOnCluster || clusterManager == STANDALONE){
        childArgs ++= args.childArgs
      }else if(clusterManager == YARN){
        for(arg <- args.childArgs){
          childArgs += ("--arg", arg)
        }
      }
    }

    // Read from default spark properties, if any
    for((k, v) <- args.getDefaultSparkProperties){
      if(!sysProps.contains(k)) sysProps(k) = v
    }

    (childArgs, childClassPath, sysProps, childMainClass)
  }

  private def launch(
                      childArgs: ArrayBuffer[String],
                      childClasspath: ArrayBuffer[String],
                      sysProps: Map[String, String],
                      childMainClass: String,
                      verbose: Boolean = false): Unit ={
    if(verbose){
      printStream.println(s"Main class:\n$childMainClass")
      printStream.println(s"arguments:\n${childArgs.mkString("\n")}")
      printStream.println(s"system properties:\n${sysProps.mkString("\n")}")
      printStream.println(s"ClassPath elements:\n${childClasspath.mkString("\n")}")
      printStream.println("\n")
    }

    val loader = new
  }
}

case class OptionAssigner(
                           value: String,
                           clusterManager: Int,
                           deployOnCluster: Boolean,
                           clOption: String = null,
                           sysProp: String = null
                           )
