package org.apache.spark


import java.io.File
import java.net.URI
import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.SparkDeploySchedulerBackend
import org.apache.spark.util.{MetadataCleanerType, MetadataCleaner, TimeStampedWeakValueHashMap, Utils}

import scala.collection.{mutable, Map, Set}

/**
 * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
 * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
 *
 * @param config a Spark Config object describing the application configuration. Any settings in
 *               this config overrides the default configs as well as system properties.
 */
class SparkContext(config: SparkConf) extends Logging {

  // This is used only by YARN for now, but should be relevant to other cluster types (Mesos,
  // etc) too. This is typically generated from InputFormatInfo.computePreferredLocations. It
  // contains a map from hostname to a list of input format splits on the host.
  private var preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()

  /**
   * Create a SparkContext that loads settings from system properties (for instance, when
   * launching with ./bin/spark-submit).
   */
  def this() = this(new SparkConf())

  /**
   * :: DeveloperApi ::
   * Alternative constructor for setting preferred locations where Spark will create executors.
   *
   * @param preferredNodeLocationData used in YARN mode to select nodes to launch containers on. Ca
   *                                  be generated using [[org.apache.spark.scheduler.InputFormatInfo.computePreferredLocations]]
   *                                  from a list of input files or InputFormats for the application.
   */
  def this(config: SparkConf, preferredNodeLocationData: Map[String, Set[SplitInfo]]) = {
    this(config)
    this.preferredNodeLocationData = preferredNodeLocationData
  }

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI
   * @param conf a [[org.apache.spark.SparkConf]] object specifying other Spark parameters
   */
  def this(master: String, appName: String, conf: SparkConf) = {
    this(SparkContext.updatedConf(conf, master, appName))
  }

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes.
   */
  def this(
            master: String,
            appName: String,
            sparkHome: String = null,
            jars: Seq[String] = Nil,
            environment: Map[String, String] = Map(),
            preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()) = {
    this(SparkContext.updatedConf(new SparkConf(), master, appName, sparkHome, jars, environment))
    this.preferredNodeLocationData = preferredNodeLocationData
  }


  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   */
  private[spark] def this(master: String, appName: String) =
    this(master, appName, null, Nil, Map(), Map())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   */
  private[spark] def this(master: String, appName: String, sparkHome: String) =
    this(master, appName, sparkHome, Nil, Map(), Map())

  /**
   * Alternative constructor that allows setting common Spark properties directly
   *
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your application, to display on the cluster web UI.
   * @param sparkHome Location where Spark is installed on cluster nodes.
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  private[spark] def this(master: String, appName: String, sparkHome: String, jars: Seq[String]) =
    this(master, appName, sparkHome, jars, Map(), Map())

  private[spark] val conf = config.clone
  conf.validateSettings()

  /**
   * Return a copy of this SparkContext's configuration. The configuration ''cannot'' be
   * changed at runtime.
   */
  def getConf = config.clone

  if (!conf.contains("spark.master")) {
    throw new SparkException("A master url must be set in your configuration")
  }

  if (!conf.contains("spark.app.name")) {
    throw new SparkException("A appName url must be set in your configuration")
  }

  if (conf.getBoolean("spark.logConf", false)) {
    logInfo("Spark configuration:\n" + conf.toDebugString)
  }

  // Set Spark driver host and port system properties
  //从这里也可以看到，driver默认的地址是本机
  conf.setIfMissing("spark.driver.host", Utils.localHostName())
  conf.setIfMissing("spark.driver.port", "0")

  val jars: Seq[String] =
    conf.getOption("spark.jars").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val files: Seq[String] =
    conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val master = conf.get("spark.master")
  val appName = conf.get("spark.app.name")

  // Generate the random name for a temp folder in Tachyon
  // Add a timestamp as the suffix here to make it more safe
  val tachyonFolderName = "spark-" + UUID.randomUUID().toString
  conf.set("spark.tachyonStore.folderName", tachyonFolderName)

  val isLocal = (master == "local" || master.startsWith("local["))

  if (master == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")

  // An asynchronous listener bus for Spark events
  private[spark] val listenerBus = new LiveListenerBus()

  //todo sparkEnv 实现
  private[spark] val env: SparkEnv = null

  // Used to store a URL for each static file/jar together with the file's local timestamp
  private[spark] val addedFiles = mutable.HashMap[String, Long]()
  private[spark] val addedJars = mutable.HashMap[String, Long]()

  // Keeps track of all persisted RDDs
  private[spark] val persistentRdds = new TimeStampedWeakValueHashMap[Int, RDD[_]]
  private[spark] val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SPARK_CONTEXT, this.cleanup, conf)

  //todo Initialize the Spark UI, registering all associated listeners

  /** A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse. */
  val hadoopConfiguration = {
    val env = SparkEnv.get
    val hadoopConf = SparkHadoopUtil.get.newConfiguration()
    // Explicitly check for S3 environment variables
    if (System.getenv("AWS_ACCESS_KEY_ID") != null &&
      System.getenv("AWS_SECRET_ACCESS_KEY") != null) {
      hadoopConf.set("fs.s3.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
      hadoopConf.set("fs.s3n.awsAccessKeyId", System.getenv("AWS_ACCESS_KEY_ID"))
      hadoopConf.set("fs.s3.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
      hadoopConf.set("fs.s3n.awsSecretAccessKey", System.getenv("AWS_SECRET_ACCESS_KEY"))
    }

    conf.getAll().foreach { case (k, v) =>
      if (k.startsWith("spark.hadoop.")) {
        hadoopConf.set(k.substring("spark.hadoop.".length), v)
      }
    }
    val buffersize = conf.get("spark.buffer.size", "65536")
    hadoopConf.set("io.file.buffer.size", buffersize)
    hadoopConf
  }

  private[spark] val eventLogger: Option[EventLoggingListener] = {
    if (conf.getBoolean("spark.eventLog.enabled", false)) {
      val logger = new EventLoggingListener(appName, conf, hadoopConfiguration)
      logger.start()
      listenerBus.addListener(logger)
      Some(logger)
    } else None
  }

  // At this point, all relevant SparkListeners have been registered, so begin releasing events
  listenerBus.start()

  val startTime = System.currentTimeMillis()

  if (jars != null) {
    jars.foreach(addJar)
  }

  if (files != null) {
    files.foreach(addFile)
  }

  private def warnSparkMem(value: String): String = {
    logWarning("Using SPARK_MEM to set amount of memory to use per executor process is " +
      "deprecated, please use spark.executor.memory instead.")
    value
  }

  private[spark] val executorMemory = conf.getOption("spark.executo.memory")
    .orElse(Option(System.getenv("")))
    .orElse(Option(System.getenv(""))).map(warnSparkMem)
    .map(Utils.memoryStringToMb)
    .getOrElse(512)

  // Environment variables to pass to our executors.
  private[spark] val executorEnvs = mutable.HashMap[String, String]()

  // Convert java options to env vars as a work around
  // since we can't set env vars directly in sbt.
  for{(envKey, propKey) <- Seq(("SPARK.TESTING", "spark.test"))
      value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))}{
    executorEnvs(envKey) = value
  }

  // The Mesos scheduler backend relies on this environment variable to set executor memory.
  // TODO: Set this only in the Mesos scheduler.
  executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
  executorEnvs ++= conf.getExecutorEnv

  // Set SPARK_USER for user who is running SparkContext.
  val sparkUser = Option{
    Option(System.getenv("SPARK_USER")).getOrElse(System.getProperty("user.name"))
  }.getOrElse(SparkContext.SPARK_UNKNOWN_USER)

  executorEnvs("SPARK_USER") = sparkUser

  private[spark] var taskScheduler = SparkContext.createTaskScheduler(this, master)

  @volatile private[spark] var dagScheduler:DAGScheduler = _

  try{
    dagScheduler = new DAGScheduler(this)
  }catch {
    case e: Exception => {
      try {
        stop()
      } finally {
        throw new SparkException("Error while constructing DAGScheduler", e)
      }
    }
  }

  /** Shut down the SparkContext. */
  def stop(): Unit ={
    postApplicationEnd()
  }

  /** Post the application end event */
  def postApplicationEnd(): Unit ={
    listenerBus.post(SparkListenerApplicationEnd(System.currentTimeMillis()))
    //todo stop ui
    //ui.stop
    val dagSchedulerCopy = dagScheduler
    dagScheduler = null
    if(dagSchedulerCopy != null){
      metadataCleaner.cancel()
      cleaner
    }
  }


  /**
   * Adds a JAR dependency for all tasks to be executed on this SparkContext in the future.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), an HTTP, HTTPS or FTP URI, or local:/path for a file on every worker node.
   */
  def addJar(path: String): Unit = {
    if (path == null) {
      logWarning("null specified as parameter to addJar")
      return
    }
    var key = ""
    if (path.contains("\\")) {
      // For local paths with backslashes on Windows, URI throws an exception
      key = env.httpFileServer.addJar(new File(path))
    } else {
      val uri = new URI(path)
      key = uri.getScheme match {
        case "file" | null =>
          if (SparkHadoopUtil.get.isYarnMode() &&
            (master == "yarn-standalone" || master == "yarn-cluster")) {
            // In order for this to work in yarn-cluster mode the user must specify the
            // --addJars option to the client to upload the file into the distributed cache
            // of the AM to make it show up in the current working directory.
            val fileName = new Path(uri.getPath).getName()
            try {
              env.httpFileServer.addJar(new File(fileName))
            } catch {
              case e: Exception =>
                // For now just log an error but allow to go through so spark examples work.
                // The spark examples don't really need the jar distributed since its also
                // the app jar.
                logError("Error adding jar (" + e + "), was the --addJars option used?")
                null
            }
          } else {
            env.httpFileServer.addJar(new File(uri.getPath))
          }
        // A JAR file which exists locally on every worker node
        case "local" =>
          "file:" + uri.getPath
        case _ =>
          path
      }
    }

    if (key != null) {
      addedJars(key) = System.currentTimeMillis()
      logInfo("Added JAR " + path + " at " + key + " with timestamp " + addedJars(key))
    }

    postEnvironmentUpdate()
  }

  /**
   * Add a file to be downloaded with this Spark job on every node.
   * The `path` passed can be either a local file, a file in HDFS (or other Hadoop-supported
   * filesystems), or an HTTP, HTTPS or FTP URI.  To access the file in Spark jobs,
   * use `SparkFiles.get(path)` to find its download location.
   */
  def addFile(path: String): Unit = {
    val uri = new URI(path)
    val key = uri.getScheme match {
      case null | "file" => env.httpFileServer.addFile(new File(uri.getPath))
      case "local" => "file:" + uri.getPath
      case _ => path
    }

    addedFiles(key) = System.currentTimeMillis()
    Utils.fetchFile(path, new File(SparkFiles.getRootDirectory()), conf, env.securityManager)
    logInfo("Added file " + path + " at " + key + " with timestamp " + addedFiles(key))
    postEnvironmentUpdate()
  }

  /** Post the environment update event once the task scheduler is ready */
  //todo 实现sparkEnv的更新
  private def postEnvironmentUpdate(): Unit = {

  }


  private[spark] def cleanup(cleanupTime: Long): Unit = {
    //定时删除过时的缓存rdd
    persistentRdds.clearOldValues(cleanupTime)
  }
}

object SparkContext extends Logging {

  private[spark] val SPARK_VERSION = "1.0.0"

  private[spark] val SPARK_UNKNOWN_USER = "<unknown>"

  private[spark] def updatedConf(
                                  conf: SparkConf,
                                  master: String,
                                  appName: String,
                                  sparkHome: String = null,
                                  jars: Seq[String] = Nil,
                                  environment: Map[String, String] = Map()
                                  ): SparkConf = {
    val res = conf.clone
    res.setMaster(master)
    res.setAppName(appName)
    if (sparkHome != null) {
      res.setSparkHome(sparkHome)
    }
    if (jars != null && !jars.isEmpty) {
      res.setJars(jars)
    }
    res.setExecutorEnv(environment.toSeq)
    res
  }

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext.
   *
   * instance: jar:file:/D:/localRepository/org/apache/commons/commons-lang3/3.3.2/commons-lang3-3.3.2.jar!/org/apache/commons/lang3/StringUtils.class
   */
  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        return Some(uriStr.substring("jar:file:".length, uriStr.indexOf("!")))
      }
    }
    None
  }

  /**
   * Find the JAR that contains the class of a particular object, to make it easy for users
   * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
   * your driver program.
   */
  def jarOfObject(obj: AnyRef): Option[String] = jarOfClass(obj.getClass)

  /** Creates a task scheduler based on a given master URL. Extracted for testing. */
  private def createTaskScheduler(sc: SparkContext, master: String): TaskScheduler = {
    // Regular expression used for local[N] and local[*] master formats
    val LOCAL_N_REGEX = """local\[([0-9\*]+)\]""".r
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    val LOCAL_N_FAILURES_REGEX = """local\[([0-9]+)\s*,\s*([0-9]+)\]""".r
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    val LOCAL_CLUSTER_REGEX = """local-cluster\[\s*([0-9]+)\s*,\s*([0-9]+)\s*,\s*([0-9]+)\s*]""".r
    // Regular expression for connecting to Spark deploy clusters
    val SPARK_REGEX = """spark://(.*)""".r
    // Regular expression for connection to Mesos cluster by mesos:// or zk:// url
    val MESOS_REGEX = """(mesos|zk)://.*""".r
    // Regular expression for connection to Simr cluster
    val SIMR_REGEX = """simr://(.*)""".r

    // When running locally, don't try to re-execute tasks on failure.
    val MAX_LOCAL_TASK_FAILURES = 1

    master match {
      case SPARK_REGEX(sparkUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)

        scheduler
    }


  }
}
