package org.apache.spark

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap

/**
 * Created by Administrator on 2017/5/23.
 */
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging {

  def this() = this(true)

  private val settings = new HashMap[String, String]()

  if (loadDefaults) {
    for ((k, v) <- System.getProperties.asScala if k.startsWith("spark.")) {
      settings(k) = v
    }
  }

  def set(key: String, value: String): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }

    settings(key) = value
    this
  }

  /**
   * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
   * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
   */
  def setMaster(master: String): SparkConf = {
    set("spark.master", master)
  }

  /** Set a name for your application. Shown in the Spark web UI. */
  def setAppName(name: String): SparkConf = {
    set("spark.app.name", name)
  }

  def setJars(jars: Seq[String]): SparkConf = {
    for (jar <- jars if jar == null) logWarning("null jar passed to SparkContext constuctor")
    set("spark.jars", jars.filter(_ != null).mkString(","))
  }

  /** Set JAR files to distribute to the cluster. (Java-friendly version.) */
  def setJars(jars: Array[String]): SparkConf = {
    setJars(jars.toSeq)
  }

  /**
   * Set an environment variable to be used when launching executors for this application.
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
   */
  def setExecutorEnv(variable: String, value: String): SparkConf = {
    set(s"spark.executorEnv.$variable", value)
  }

  /**
   * Set multiple environment variables to be used when launching executors.
   * These variables are stored as properties of the form spark.executorEnv.VAR_NAME
   * (for example spark.executorEnv.PATH) but this method makes them easier to set.
   */
  def setExecutorEnv(variables: Seq[(String, String)]): SparkConf = {
    for ((k, v) <- variables) {
      setExecutorEnv(k, v)
    }
    this
  }

  def setExecutorEnv(variables: Array[(String, String)]): SparkConf = {
    setExecutorEnv(variables.toSeq)
  }

  /**
   * Set the location where Spark is installed on worker nodes.
   */
  def setSparkHome(home: String): SparkConf = {
    set("spark.home", home)
  }

  def setAll(settings: Traversable[(String, String)]): SparkConf = {
    this.settings ++= settings
    this
  }

  def setIfMissing(key: String, value: String): SparkConf = {
    if (!settings.contains(key)) {
      settings(key) = value;
    }
    this
  }

  def remove(key: String): SparkConf = {
    settings.remove(key)
    this
  }

  def get(key: String): String = {
    settings.getOrElse(key, throw new NoSuchElementException(key))
  }

  def get(key: String, defaultValue: String): String = {
    settings.getOrElse(key, defaultValue)
  }

  def getOption(key:String):Option[String] = {
    settings.get(key)
  }

  def getAll():Array[(String, String)] = settings.clone().toArray

  def getInt(key:String, defaultValue:Int):Int = {
    settings.get(key).map(_.toInt).getOrElse(defaultValue)
  }


  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  def getExecutorEnv:Seq[(String, String)] = {
    val prefix = "spark.executorEnv."
    getAll().filter{case (k, v) => k.startsWith(prefix)}.map{
      case (k, v) => (k.substring(prefix.length), v)
    }
  }

  def getAkkaConf():Seq[(String, String)] = {
    getAll().filter{case (k, v) => k.startsWith("akka.")}
  }

  def contains(key:String):Boolean = settings.contains(key)

  override def clone:SparkConf = {
    new SparkConf(false).setAll(settings)
  }

  private[spark] def validateSettings(): Unit ={
    if (settings.contains("spark.local.dir")) {
      val msg = "In Spark 1.0 and later spark.local.dir will be overridden by the value set by " +
        "the cluster manager (via SPARK_LOCAL_DIRS in mesos/standalone and LOCAL_DIRS in YARN)."
      logWarning(msg)
    }

    val executorOptsKey = "spark.executor.extraJavaOptions"
    val executorClasspathKey = "spark.executor.extraClassPath"
    val driverOptsKey = "spark.driver.extraJavaOptions"
    val driverClassPathKey = "spark.driver.extraClassPath"

    settings.get(executorOptsKey).map{javaOpts =>
      if(javaOpts.contains("-Dspark")){
        val msg = s"$executorOptsKey is not allowed to set Spark options (was '$javaOpts)'. " +
          "Set them directly on a SparkConf or in a properties file when using ./bin/spark-submit."
        throw new Exception(msg)
      }
      if(javaOpts.contains("-Xmx"))
    }

    sys.env.get("SPARK_JAVA_OPTS").foreach{value =>
      val warnings =
      s"""
         |SPARK_JAVA_OPTS was detected (set to '$value').
         |This is deprecated in Spark 1.0+.
         |
 |Please instead use:
         | - ./spark-submit with conf/spark-defaults.conf to set defaults for an application
         | - ./spark-submit with --driver-java-options to set -X options for a driver
         | - spark.executor.extraJavaOptions to set -X options for executors
         | - SPARK_DAEMON_JAVA_OPTS to set java options for standalone daemons (master or worker)
       """.stripMargin
      logWarning(warnings)

      for(key <- Seq(executorOptsKey, driverOptsKey)){
        if(getOption(key).isDefined){
          throw new SparkException(s"Found both $key and SPARK_JAVA_OPTS. Use only the former.")
        }else {
          logWarning(s"Setting '$key' to '$value' as a work-around.")
          set(key, value)
        }
      }
    }
    sys.env.get("SPARK_CLASSPATH").foreach { value =>
      val warning =
        s"""
           |SPARK_CLASSPATH was detected (set to '$value').
           |This is deprecated in Spark 1.0+.
           |
          |Please instead use:
           | - ./spark-submit with --driver-class-path to augment the driver classpath
           | - spark.executor.extraClassPath to augment the executor classpath
        """.stripMargin
      logWarning(warning)

      for (key <- Seq(executorClasspathKey, driverClassPathKey)) {
        if (getOption(key).isDefined) {
          throw new SparkException(s"Found both $key and SPARK_CLASSPATH. Use only the former.")
        } else {
          logWarning(s"Setting '$key' to '$value' as a work-around.")
          set(key, value)
        }
      }
    }
  }

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
   */
  def toDebugString: String = {
    settings.toArray.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }

}
