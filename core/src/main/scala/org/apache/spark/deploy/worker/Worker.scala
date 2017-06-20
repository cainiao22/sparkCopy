package org.apache.spark.deploy.worker

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import akka.remote.RemotingLifecycleEvent
import org.apache.spark.deploy._
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.metrics.MetricSystem

import scala.collection.mutable
import scala.concurrent.duration._

import akka.actor.{Cancellable, Address, ActorSelection, Actor}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkException, Logging, SparkConf}

/**
 * @param masterUrls Each url should look like spark://host:port.
 */
private[spark] class Worker(
                             host: String,
                             port: Int,
                             webUiPort: Int,
                             cores: Int,
                             memory: Int,
                             masterUrls: Array[String],
                             actorSystemName: String,
                             actorName: String,
                             workDirPath: String = null,
                             val conf: SparkConf,
                             val securityMgr: SecurityManager
                             ) extends Actor with Logging {

  import context.dispatcher

  Utils.checkHost(host, "Expected hostname")
  assert(port > 0)

  def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss") // For worker and executor IDs

  //todo ??? 为啥要除以4？
  val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4

  val REGISTRATION_TIMEOUT = 20.seconds
  val REGISTRATION_RETRIES = 3

  val CLEANUP_ENABLED = conf.getBoolean("spark.worker.cleanup.enabled", false)
  // How often worker will clean up old app folders
  val CLEANUP_INTERVAL_MILLIS = conf.getLong("spark.worker.cleanup.interval", 60 * 30) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  val APP_DATA_RETENTION_SECS = conf.getLong("spark.worker.ckeanup.appDataTtl", 7 * 24 * 3600)

  val masterLock: Object = new Object
  var master: ActorSelection = null
  var masterAddress: Address = null
  var activeMasterUrl: String = ""
  var activeMasterWebUiUrl: String = ""
  val akkaUrl = "akka.tcp://%s@%s:%s/user/%s".format(actorSystemName, host, port, actorName)
  @volatile var registered = false
  @volatile var connected = false
  val workerId: String = generateWorkerId()
  val sparkHome = new File(Option(System.getenv("SPARK_HOME")).getOrElse("."))
  var workDir: File = null
  val executors = new mutable.HashMap[String, ExecutorRunner]
  val finishedExecutors = new mutable.HashMap[String, ExecutorRunner]
  val drivers = new mutable.HashMap[String, DriverRunner]
  val finishedDrivers = new mutable.HashMap[String, DriverRunner]

  val publicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  //todo webUI of worker
  //var webUi:workerWeb

  var coresUsed = 0
  var memoryUsed = 0

  val metricsSystem = MetricSystem.createMetricsSystem("worker", conf, securityMgr)
  val workerSource = new WorkerSource(this)

  var registrationRetryTimer: Option[Cancellable] = None

  def coresFree: Int = cores - coresUsed

  def memoryFree: Int = memory - memoryUsed

  def createWorkDir(): Unit = {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(sparkHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      workDir.mkdirs()
      if (!workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(-1)
      }
      assert(workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }


  override def preStart(): Unit = {
    assert(!registered)
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo("spark home: " + sparkHome)
    createWorkDir()
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    //TODO webui 创建

    registerWithMaster()
    metricsSystem.registerSource(workerSource)
    metricsSystem.start()

  }

  def changeMaster(url:String, uiUrl:String): Unit ={
    masterLock.synchronized{
      activeMasterUrl = url
      activeMasterWebUiUrl = uiUrl
      master = context.actorSelection(Master.toAkkaUrl(url))
      masterAddress = activeMasterUrl match {
        case Master.sparkUrlRegex(_host, _port) =>
          Address("akka.tcp", Master.systemName, _host, _port.toInt)
        case x =>
          throw new SparkException("Invalid spark URL: " + x)
      }
      connected = true
    }
  }

  def tryRegisterAllMasters(): Unit = {
    for (masterUrl <- masterUrls) {
      logInfo("Connecting to master " + masterUrl + "...")
      val actor = context.actorSelection(Master.toAkkaUrl(masterUrl))
      //todo work webUI#boundPort
      val boundPort = 0
      actor ! RegisterWorker(workerId, host, port, cores, memory, boundPort, publicAddress)
    }
  }

  def registerWithMaster(): Unit = {
    tryRegisterAllMasters()
    var retries = 0
    registrationRetryTimer = Some {
      context.system.scheduler.schedule(REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT) {
        Utils.tryOrExit {
          retries += 1
          if (registered) {
            registrationRetryTimer.foreach(_.cancel())
          } else if (retries >= REGISTRATION_RETRIES) {
            logError("All masters are unresponsive! Giving up.")
            System.exit(1)
          } else {
            tryRegisterAllMasters()
          }
        }
      }
    }
  }

  override def receive = {
    case RegisteredWorker(masterUrl, masterWebUiUrl) =>
      logInfo("Successfully registered with master " + masterUrl)
      registered = true
      changeMaster(masterUrl, masterWebUiUrl)
      context.system.scheduler.schedule(0 millis, HEARTBEAT_MILLIS millis, self, SendHeartbeat)
      if(CLEANUP_ENABLED){
        context.system.scheduler.schedule(CLEANUP_INTERVAL_MILLIS millis,
        CLEANUP_INTERVAL_MILLIS millis, self, WorkDirCleanup)
      }

    case SendHeartbeat =>
      masterLock.synchronized{
        if(connected) {master ! Heartbeat(workerId)}
      }

    case WorkDirCleanup =>
      val cleanupFuture = concurrent.future{
        logInfo("Cleaning up oldest application directories in " + workDir + " ...")
        Utils.findOldestFiles(workDir, APP_DATA_RETENTION_SECS)
        .foreach(Utils.deleteRecursively)
      }

      cleanupFuture onFailure {
        case e: Throwable =>
          logError("App dir cleanup failed: " + e.getMessage, e)
      }

    case MasterChanged(masterUrl, masterWebUiUrl) =>
      logInfo("Master has changed, new master is at " + masterUrl)
      changeMaster(masterUrl, masterWebUiUrl)

      val execs = executors.values
        .map(e => new ExecutorDescription(e.appId, e.execId, e.cores, e.state))
      sender ! WorkerSchedulerStateResponse(workerId, execs.toList, drivers.keys.toSeq)

    case RegisterWorkerFailed(message) =>
      if(!registered){
        logError("Worker registration failed: " + message)
        System.exit(-1)
      }

    case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
      if(masterUrl != activeMasterUrl){
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      }else{
        try{
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))
          val manager = new ExecutorRunner(appId, execId, appDesc, cores_, memory_,
            self, workerId, host,
            appDesc.sparkHome.map(userSparkHome => new File(userSparkHome)).getOrElse(sparkHome),
            workDir, akkaUrl, ExecutorState.RUNNING)
          executors(appId + "/" + execId) = manager
          manager.start()
          coresUsed += cores_
          memoryUsed += memory_
          masterLock.synchronized{
            master ! ExecutorStateChanged(appId, execId, manager.state, None, None)
          }
        }catch {
          case e:Exception =>
            logError("Failed to launch executor %s/%d for %s".format(appId, execId, appDesc.name))
            if(executors.contains(appId + "/" + execId)){
              executors(appId + "/" + execId).kill()
            }

            masterLock.synchronized{
              master ! ExecutorStateChanged(appId, execId, ExecutorState.FAILED, None, None)
            }
        }
      }

    case LaunchDriver(driverId, driverDesc) =>
      logInfo(s"Asked to launch driver $driverId")
      val driver = new DriverRunner(driverId, workDir, sparkHome, driverDesc, self, akkaUrl)
      drivers(driverId) = driver
      driver.start()

      coresUsed += driverDesc.cores
      memoryUsed += driverDesc.mem

    case DriverStateChanged(driverId, state, exception) =>
      state match {
        case DriverState.ERROR =>
          logWarning(s"Driver $driverId failed with unrecoverable exception: ${exception.get}")
        case DriverState.FINISHED =>
          logInfo(s"Driver $driverId exited successfully")
        case DriverState.KILLED =>
          logInfo(s"Driver $driverId was killed by user")
      }

      masterLock.synchronized{
        master ! DriverStateChanged(driverId, state, exception)
      }

      val driver = drivers.remove(driverId).get
      finishedDrivers(driverId) = driver
      memoryUsed -= driver.driverDesc.mem
      coresUsed -= driver.driverDesc.cores

  }

  def generateWorkerId(): String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date), host, port)
  }

}
