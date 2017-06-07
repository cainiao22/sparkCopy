package org.apache.spark.deploy.master

import java.text.SimpleDateFormat

import akka.actor.{Props, Cancellable, ActorRef, Actor}
import akka.remote.RemotingLifecycleEvent
import akka.serialization.SerializationExtension
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.deploy.master.MasterMessages.{ElectedLeader, CheckForWorkerTimeOut}
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.metrics.MetricSystem
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkException, Logging}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

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

  var persistenceEngine: PersistenceEngine = _

  var leaderElectionAgent: ActorRef = _

  private var recoveryCompletionTask: Cancellable = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  override def preStart(): Unit = {
    logInfo("starting spark master at " + masterUrl)
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    //todo WebUI#bind()
    //webUi.bind()
    //masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort

    context.system.scheduler.schedule(0 millis, WORKER_TIMEOUT millis, self, CheckForWorkerTimeOut)
    masterMetricsSystem.registerSource(masterSource)
    masterMetricsSystem.start()
    applicationMetricsSystem.start()

    persistenceEngine = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        new ZooKeeperPersistenceEngine(SerializationExtension(context.system), conf)
      case "FILESYSTEM" =>
        logInfo("Persisting recovery state to directory: " + RECOVERY_DIR)
        new FileSystemPersistenceEngine(RECOVERY_DIR, SerializationExtension(context.system))
      case _ =>
        new BlackHolePersistenceEngine
    }

    leaderElectionAgent = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        context.actorOf(Props(classOf[ZooKeeperLeaderElectionAgent], self, masterUrl, conf))
      case _ =>
        context.actorOf(Props(classOf[MonarchyLeaderAgent], self))
    }
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message) // calls postStop()!
    logError("Master actor restarted due to exception", reason)
  }

  override def postStop(): Unit = {
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel()
    }

    //todo webUI
    //webUi.close
    fileSystemUsed.foreach(_.close())
    masterMetricsSystem.stop()
    applicationMetricsSystem.stop()
    persistenceEngine.close()
    context.stop(leaderElectionAgent)
  }

  //重点来了
  override def receive = {
    //重新选举
    case ElectedLeader =>
      val (storedApps, storedDrivers, storedWorkers) = persistenceEngine.readPersistedData()
      state = if (storedApps.isEmpty && storedDrivers.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected leader! New state: " + state)
      if(state == RecoveryState.RECOVERING){
        //恢复持久化的app、driver、worker
      }

  }


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
