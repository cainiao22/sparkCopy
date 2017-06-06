package org.apache.spark.deploy.master

import java.text.SimpleDateFormat

import akka.actor.{Cancellable, ActorRef, Actor}
import akka.remote.RemotingLifecycleEvent
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.metrics.MetricSystem
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkException, Logging}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] class Master(
                             host: String,
                             port: Int,
                             webUiPort: Int,
                             val securityMgr: SecurityManager
                             ) extends Actor with Logging {

  import context.dispatcher

  // to use Akka's scheduler.schedule()

  val conf = new SparkConf()

  def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")

  // For application IDs
  val WORKER_TIMEOUT = conf.getLong("spark.worker.timeout", 60) * 1000
  val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
  val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
  val RECOVERY_DIR = conf.get("spark.deploy.recoveryDirectory", "")
  val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")

  val workers = new mutable.HashSet[WorkerInfo]
  val idToWorker = new mutable.HashMap[String, WorkerInfo]
  val addressToWorker = new mutable.HashMap[String, WorkerInfo]

  val apps = new mutable.HashSet[ApplicationInfo]
  val idToApp = new mutable.HashMap[String, ApplicationInfo]
  val actorToApp = new mutable.HashMap[ActorRef, ApplicationInfo]
  val waitingApps = new ArrayBuffer[ApplicationInfo]
  val completedApps = new ArrayBuffer[ApplicationInfo]
  var nextAppNumber = 0

  val appIdToUI = new mutable.HashMap[String, SparkUI]
  val fileSystemUsed = new mutable.HashSet[FileSystem]

  val drivers = new mutable.HashSet[DriverInfo]
  val completedDrivers = new ArrayBuffer[DriverInfo]
  val waitingDrivers = new ArrayBuffer[DriverInfo]
  var nextDriverNumber = 0

  Utils.checkHost(host, "Expected hostname")

  val masterMetricsSystem = MetricSystem.createMetricsSystem("master", conf, securityMgr)
  val applicationMetricsSystem = MetricSystem.createMetricsSystem("application", conf, securityMgr)

  val masterSource = new MasterSource(this)

  val webUi = new MasterWebUI(this, webUiPort)

  val masterPublicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  val masterUrl = "spark://" + host + ":" + port
  val masterWebUiUrl: String = _

  var state = RecoveryState.STANDBY

  var persistenceEngine:PersistenceEngine = _

  var leaderElectionAgent:ActorRef = _

  private var recoveryCompletionTask:Cancellable = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  override def preStart(): Unit ={
    logInfo("starting spark master at " + masterUrl)
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    webUi.bind()
  }

  override def receive: Receive = ???
}


private[spark] object Master {
  val systemName = "sparkMaster"
  private val actorName = "Master"
  val sparkUrlRegex = "spark://([^:]+):([0-9]+)".r

  def toAkkaUrl(sparkUrl: String): String = {
    sparkUrl match {
      case sparkUrlRegex(host, port) =>
        "akka.tcp:://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
      case _ =>
        throw new SparkException("Invalid master URL: " + sparkUrl)
    }
  }
}
