package org.apache.spark.deploy.master

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor._
import akka.remote.RemotingLifecycleEvent
import akka.serialization.SerializationExtension
import org.apache.hadoop.fs.FileSystem

import org.apache.spark.deploy.master.DriverState.DriverState
import org.apache.spark.deploy._
import org.apache.spark.deploy.master.MasterMessages.{CompleteRecovery, ElectedLeader, CheckForWorkerTimeOut}
import org.apache.spark.deploy.master.ui.MasterWebUI
import org.apache.spark.metrics.MetricSystem
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkException, Logging}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.Random

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

    case RegisterWorker(id, workerHost, workerPort, cores, memory, workerUiPort, publicAddress) => {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
      if (state == RecoveryState.STANDBY) {

      } else if (idToWorker.contains(id)) {
        sender ! RegisterWorkerFailed("Duplicate worker ID")
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          sender, workerUiPort, publicAddress)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          sender ! RegisteredWorker(masterUrl, masterWebUiUrl)
          schedule()
        } else {
          val workerAddress = worker.actor.path.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          sender ! RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress)
        }
      }
    }

    case RequestSubmitDriver(description) =>
      if(state != RecoveryState.ALIVE){
        val msg = s"Can only accept driver submissions in ALIVE state. Current state: $state."
        sender ! SubmitDriverResponse(false, None, msg)
      }else{
        logInfo("Driver sumitted" + description.command.mainClass)
        val driver = createDriver(description)
        persistenceEngine.addDriver(driver)
        waitingDrivers += driver
        drivers.add(driver)
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".

        sender ! SubmitDriverResponse(true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}")
      }

    case RequestDriverStatus(driverId) =>
      (drivers ++ completedDrivers).find(_.id == driverId) match {
        case Some(driver) =>
          sender ! DriverStatusResponse(found = true, Some(driver.state),
            driver.worker.map(_.id), driver.worker.map(_.hostPort), driver.exception)
        case None =>
          sender ! DriverStatusResponse(found = false, None, None, None, None)
      }

    case RegisterApplication(description) =>
      if(state == RecoveryState.STANDBY){
        //do nothing
      }else{
        logInfo("register app " + description.name)
        //在这里 确认driver是谁
        val app = createApplication(description, sender)
        registerApplication(app)
        logInfo("register app" + description.name + " with id " + app.id)
        persistenceEngine.addApplication(app)
        sender ! RegisteredApplication(app.id, masterUrl)
        schedule()
      }

    case ExecutorStateChanged(appId, execId, state, message, exitStatus) =>
      val execOption = idToApp.get(appId).flatMap(app => app.executors.get(execId))
      execOption match {
        case Some(exec) =>
          exec.state = state
          exec.application.driver ! ExecutorUpdated(execId, state, message, exitStatus)
          if(ExecutorState.isFinished(state)){

          }
      }

    case Heartbeat(workerId) =>
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartbeat = System.currentTimeMillis()
        case None =>
          logWarning("Got heartbeat from unregistered worker " + workerId)
      }

    case MasterChangeAcknowledged(appId) =>
      idToApp.get(appId) match {
        case Some(app) =>
          logInfo("Application has been re-registered: " + appId)
          app.state = ApplicationState.WAITING
        case None =>
          logWarning("Master change ack from unknown app: " + appId)
      }

      if (canCompleteRecovery) {
        completeRecovery()
      }

  }

  def canCompleteRecovery =
    apps.count(_.state == ApplicationState.UNKNOWN) == 0 &&
      workers.count(_.state == WorkerState.UNKNOWN) == 0

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

  def completeRecovery(): Unit = {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    synchronized {
      if (state != RecoveryState.RECOVERING) {
        return
      }
      state = RecoveryState.COMPLETING_RECOVERY
    }

    workers.filter(_.state == WorkerState.UNKNOWN).foreach(removeWorker)
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach(finishApplication)

    drivers.filter(_.worker.isEmpty).foreach { d =>
      logWarning(s"Driver ${d.id} was not found after master recovery")
      if (d.desc.supervise) {
        logWarning(s"Re-launching ${d.id}")
        relaunchDriver(d)
      } else {
        removeDriver(d.id, DriverState.ERROR, None)
        logWarning(s"Did not re-launch ${d.id} because it was not supervised")
      }
    }

    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  def finishApplication(app: ApplicationInfo): Unit = {
    removeApplication(app, ApplicationState.FINISHED)
  }

  def removeApplication(app: ApplicationInfo, state: ApplicationState.Value): Unit = {
    if (apps.contains(app)) {
      logInfo("removing app " + app.id)
      apps -= app
      actorToApp -= app.driver
      addressToApp -= app.driver.path.address
      if (completedApps.size >= RETAINED_APPLICATIONS) {
        val toRemove = math.max(RETAINED_APPLICATIONS / 10, 1)
        completedApps.take(toRemove).foreach(a => {
          //todo webUI#detachSparkUI实现
          //appIdToUI.remove(a.id).foreach{ui => webUi.detachSparkUI(ui)}
          applicationMetricsSystem.removeSource(app.appSource)
        })
        completedApps.trimStart(toRemove)
      }
      completedApps += app
      waitingApps -= app

      //todo rebuild sparkUI


      for (exec <- app.executors.values) {
        exec.worker.removeExecutor(exec)
        exec.worker.actor ! KillExecutor(masterUrl, app.id, exec.id)
        exec.state = ExecutorState.KILLED
      }

      app.markFinished(state)
      if (state != ApplicationState.FINISHED) {
        //不是正常退出，通知driver进程（appClient实现是"自杀"）
        app.driver ! ApplicationRemoved(state.toString)
      }
      persistenceEngine.removeApplication(app)
      schedule()
      logInfo("Recovery complete - resuming operations!")
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

    for (driver <- worker.drivers.values) {
      if (driver.desc.supervise) {
        logInfo(s"Re-launching ${driver.id}")
        relaunchDriver(driver)
      } else {
        logInfo(s"Not re-launching ${driver.id} because it was not supervised")
        removeDriver(driver.id, DriverState.ERROR, None)
      }
    }
    persistenceEngine.removeWorker(worker)
  }

  def relaunchDriver(driver: DriverInfo): Unit = {
    driver.worker = None
    driver.state = DriverState.RELAUNCHING
    waitingDrivers += driver
    schedule()
  }

  def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.path.address
    //todo ??? 为啥判断addressToworker？什么时候删除？什么时候添加的？下面没有啊
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

  def newDriverId(submitDate:Date):String = {
    val appId = "driver-%s-%04d".format(createDateFormat.format(submitDate), nextDriverNumber)
    nextDriverNumber += 1
    appId
  }

  def createDriver(desc:DriverDescription):DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }

  def launchDriver(worker: WorkerInfo, driver: DriverInfo): Unit = {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    worker.addDriver(driver)
    driver.worker = Some(worker)
    worker.actor ! LaunchDriver(driver.id, driver.desc)
    driver.state = DriverState.RUNNING
  }

  def removeDriver(driverId: String, finalState: DriverState, exception: Option[Exception]): Unit = {
    drivers.find { d => d.id == driverId } match {
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

  /** Generate a new app ID given a app's submission date */
  def newApplicationId(submitDate:Date):String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  def createApplication(desc:ApplicationDescription, driver:ActorRef):ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new ApplicationInfo(now, newApplicationId(date), desc, date, driver, defaultCores)
  }

  /**
   * Can an app use the given worker? True if the worker has enough memory and we haven't already
   * launched an executor for the app on it (right now the standalone backend doesn't like having
   * two executors on the same worker).
   */
  def canUse(app: ApplicationInfo, worker: WorkerInfo): Boolean = {
    worker.memoryFree >= app.desc.memoryPerSlave && !worker.hasExecutor(app)
  }

  /**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   */
  private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) {
      return
    }

    // First schedule drivers, they take strict precedence over applications
    val shuffledWorkers = Random.shuffle(workers) // Randomization helps balance drivers
    for (worker <- shuffledWorkers if worker.state == WorkerState.ALIVE) {
      for (driver <- waitingDrivers) {
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          launchDriver(worker, driver)
          //这个可以一边遍历，一边删数据？
          waitingDrivers -= driver
        }
      }
    }
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    if (spreadOutApps) {
      for (app <- waitingApps if app.coresLeft > 0) {
        val useableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
          .filter(canUse(app, _)).sortBy(_.coresFree).reverse
        val numUseable = useableWorkers.length
        val assigned = new Array[Int](numUseable)
        var toAssign = math.min(app.coresLeft, useableWorkers.map(_.coresFree).sum)
        var pos = 0
        while (toAssign > 0) {
          if (useableWorkers(pos).coresFree - assigned(pos) > 0) {
            assigned(pos) += 1
            toAssign -= 1
          }
          pos = (pos + 1) % numUseable
        }

        // Now that we've decided how many cores to give on each node, let's actually give them
        for (pos <- 0 until numUseable) {
          if (assigned(pos) > 0) {
            val exec = app.addExecutor(useableWorkers(pos), assigned(pos))
            launchExecutor(useableWorkers(pos), exec)
            app.state = ApplicationState.RUNNING
          }
        }
      }
    }else{
      //这里没有去判断是否已经运行application了
      for(worker <- workers if worker.coresFree > 0 && worker.state == WorkerState.ALIVE) {
        for(app <- waitingApps if app.coresLeft > 0){
          //在这里判断的。。靠
          if(canUse(app, worker)){
            val coresToUse = math.min(worker.coresFree, app.coresLeft)
            if(coresToUse > 0){
              val exec = app.addExecutor(worker, coresToUse)
              launchExecutor(worker, exec)
              app.state = ApplicationState.RUNNING
            }
          }
        }
      }
    }
  }

  def launchExecutor(worker: WorkerInfo, exec: ExecutorInfo) {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)
    worker.actor ! LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory)
    exec.application.driver ! ExecutorAdded(
      exec.id, worker.id, worker.host, exec.cores, exec.memory)
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
