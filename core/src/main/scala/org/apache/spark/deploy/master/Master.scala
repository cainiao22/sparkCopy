package org.apache.spark.deploy.master

import java.text.SimpleDateFormat

import akka.actor._
import akka.remote.RemotingLifecycleEvent
import akka.serialization.SerializationExtension
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy.{ExecutorState, ExecutorUpdated, MasterChanged}
import org.apache.spark.deploy.master.MasterMessages.{CompleteRecovery, ElectedLeader, CheckForWorkerTimeOut}
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
  val addressToWorker = new mutable.HashMap[Address, WorkerInfo]

  val apps = new mutable.HashSet[ApplicationInfo]
  val idToApp = new mutable.HashMap[String, ApplicationInfo]
  val actorToApp = new mutable.HashMap[ActorRef, ApplicationInfo]
  val addressToApp = new mutable.HashMap[Address, ApplicationInfo]()
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
      if (state == RecoveryState.RECOVERING) {
        //恢复持久化的app、driver、worker
        beginRecovery(storedApps, storedDrivers, storedWorkers)
        recoveryCompletionTask = context.system.scheduler.scheduleOnce(WORKER_TIMEOUT millis, self, CompleteRecovery)
      }

    case CompleteRecovery =>
      //todo completeRecovery

  }

  def beginRecovery(storedApps: Seq[ApplicationInfo], storedDrivers: Seq[DriverInfo],
                    storedWorkers: Seq[WorkerInfo]): Unit = {
    for (app <- storedApps) {
      logInfo("trying to recover app:" + app.id)
      try {
        registerApplication(app)
        app.state = ApplicationState.UNKNOWN
        app.driver ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("App " + app.id + " had exception on reconnect")
      }
    }

    for (driver <- drivers) {
      drivers += driver
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.actor ! MasterChanged(masterUrl, masterWebUiUrl)
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.filter { w =>
      (worker.host == w.host && worker.port == w.port) && (w.state == WorkerState.DEAD)
    }.foreach { w => workers -= w }

    val workerAddress = worker.actor.path.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker)
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  def removeWorker(worker: WorkerInfo): Unit = {
    logInfo("Removing worker " + worker.id + " on " + worker.host + ":" + worker.port)
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.actor.path.address
    worker.executors.values.foreach { exec =>
      logInfo("Telling app of lost executor: " + exec.id)
      exec.application.driver ! ExecutorUpdated(
        exec.id, ExecutorState.LOST, Some("work lost"), None)

      exec.application.removeExecutor(exec)
    }

    for(driver <- worker.drivers.values){
      if(driver.desc.supervise){
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      }else{
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    persistenceEngine.removeWorker(worker)
  }

  def relaunchDriver(driver:DriverInfo): Unit ={
    driver.worker = None
    driver.state = DriverState.RELAUNCHING
    waitingDrivers += driver
    //todo schedule实现
    //schedule()
  }

  def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.path.address
    if (addressToWorker.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    idToApp(app.id) = app
    actorToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
  }

  def removeDriver(driverId:String, finalState:DriverState, exception:Option[Exception]): Unit ={
    drivers.find{d => d.id == driverId} match {
      case Some(driver) =>
        logInfo(s"Removing driver: $driverId")
        drivers -= driver
        completedDrivers += driver
        persistenceEngine.removeDriver(driver)
        driver.state = finalState
        driver.exception = exception
        driver.worker.foreach(_.removeDriver(driver))
      case None =>
        logWarning(s"Asked to remove unknown driver: $driverId")
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
