package org.apache.spark


import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{LiveListenerBus, SplitInfo}
import org.apache.spark.util.{MetadataCleanerType, MetadataCleaner, TimeStampedWeakValueHashMap, Utils}

import scala.collection.{mutable, Map, Set}
/**
 * Main entry point for Spark functionality. A SparkContext represents the connection to a Spark
 * cluster, and can be used to create RDDs, accumulators and broadcast variables on that cluster.
 *
 * @param config a Spark Config object describing the application configuration. Any settings in
 *   this config overrides the default configs as well as system properties.
 */
class SparkContext(config: SparkConf) extends Logging {

  // This is used only by YARN for now, but should be relevant to other cluster types (Mesos,
  // etc) too. This is typically generated from InputFormatInfo.computePreferredLocations. It
  // contains a map from hostname to a list of input format splits on the host.
  private var preferredNodeLocationData:Map[String, Set[SplitInfo]] = Map()

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
   * be generated using [[org.apache.spark.scheduler.InputFormatInfo.computePreferredLocations]]
   * from a list of input files or InputFormats for the application.
   */
  def this(config:SparkConf, preferredNodeLocationData:Map[String, Set[SplitInfo]]) = {
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
  def this(master:String, appName:String, conf:SparkConf) = {
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
            preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map()) =
  {
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

  if(!conf.contains("spark.master")){
    throw new SparkException("A master url must be set in your configuration")
  }

  if(!conf.contains("spark.app.name")){
    throw new SparkException("A appName url must be set in your configuration")
  }

  if (conf.getBoolean("spark.logConf", false)) {
    logInfo("Spark configuration:\n" + conf.toDebugString)
  }

  // Set Spark driver host and port system properties
  //从这里也可以看到，driver默认的地址是本机
  conf.setIfMissing("spark.driver.host", Utils.localHostName())
  conf.setIfMissing("spark.driver.port", "0")

  val jars:Seq[String] =
    conf.getOption("spark.jars").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val files:Seq[String] =
    conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val master = conf.get("spark.master")
  val appName = conf.get("spark.app.name")

  // Generate the random name for a temp folder in Tachyon
  // Add a timestamp as the suffix here to make it more safe
  val tachyonFolderName = "spark-" + UUID.randomUUID().toString
  conf.set("spark.tachyonStore.folderName", tachyonFolderName)

  val isLocal = (master == "local" || master.startsWith("local["))

  if(master == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")

  // An asynchronous listener bus for Spark events
  private[spark] val listenerBus = new LiveListenerBus()

  //todo sparkEnv 实现
  private[spark] val env = null

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
    val env = SparkEnv.get()
  }



  private[spark] def cleanup(cleanupTime:Long): Unit ={
    //定时删除过时的缓存rdd
    persistentRdds.clearOldValues(cleanupTime)
  }
}

object SparkContext extends Logging {

  private[spark] def updatedConf(
                               conf: SparkConf,
                               master:String,
                               appName:String,
                               sparkHome:String = null,
                               jars:Seq[String] = Nil,
                               environment:Map[String, String] = Map()
                                 ):SparkConf = {
    val res = conf.clone
    res.setMaster(master)
    res.setAppName(appName)
    if(sparkHome != null){
      res.setSparkHome(sparkHome)
    }
    if(jars != null && !jars.isEmpty){
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
  def jarOfClass(cls:Class[_]):Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if(uri != null){
      val uriStr = uri.toString
      if(uriStr.startsWith("jar:file:")){
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
  def jarOfObject(obj:AnyRef):Option[String] = jarOfClass(obj.getClass)
}
