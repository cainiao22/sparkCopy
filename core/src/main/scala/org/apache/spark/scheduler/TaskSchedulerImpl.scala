package org.apache.spark.scheduler

import java.util.Timer
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a LocalBackend and setting isLocal to true.
 * It handles common logic, like determining a scheduling order across jobs, waking up to launch
 * speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * runTasks method.
 *
 * THREADING: SchedulerBackends and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * SchedulerBackends synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 */
private[spark] class TaskSchedulerImpl(val sc: SparkContext,
                                       val maxTaskFailures: Int,
                                       isLocal: Boolean = false)

  extends TaskScheduler with Logging {

  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

  val conf = sc.conf

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL = conf.getLong("spark.speculation.interval", 100)

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT = conf.getLong("spark.starvation.timeout", 15000)

  // CPUs to request per task
  val CPUS_PER_TASK = conf.getInt("spark.task.cpus", 1)

  val activeTaskSets = new mutable.HashMap[String, TaskSetManager]()

  val taskIdToTaskSetId = new mutable.HashMap[Long, String]()
  val taskIdToExecutorId = new mutable.HashMap[Long, String]()

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false

  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId: Long = new AtomicLong(0)

  // Which executor IDs we have executors on
  val activeExecutorIds = new mutable.HashSet[String]()

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  private val executorsByHost = new mutable.HashMap[String, mutable.HashSet[String]]

  private val executorIdToHost = new mutable.HashMap[String, String]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  val mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker

  var schedulableBuilder: SchedulableBuilder = null

  var rootPool: Pool = null

  // default scheduler is FIFO
  private val schedulingModeConf = conf.get("spark.scheduler.mode", "FIFO")

  val schedulingMode: SchedulingMode = try {
    SchedulingMode.withName(schedulingModeConf.toUpperCase)
  } catch {
    case e: java.util.NoSuchElementException =>
      throw new SparkException(s"Unrecognized spark.scheduler.mode: $schedulingModeConf")
  }

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  def initialize(backend: SchedulerBackend): Unit = {
    this.backend = backend
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
      }
    }

    schedulableBuilder.buildPools()
  }


  override def start() = {
    backend.start
  }


  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    SparkEnv.set(sc.env)
    // Mark each slave as alive and remember its hostname
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new mutable.HashSet[String]
        executorAdded(o.executorId, o.host)
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    val shuffledOffers = Random.shuffle(offers)
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = shuffledOffers.map(_.cores).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue()
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
    }

    //Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    //这里相当于双重循环，taskSet为外层，maxLocality为内层
    var launchedTask = false
    for (tasksetManager <- sortedTaskSets; maxLocality <- TaskLocality) {
      do {
        launchedTask = false
        for (i <- 0 until shuffledOffers.size) {
          val execId = shuffledOffers(i).executorId
          val host = shuffledOffers(i).host
          if (availableCpus(i) >= CPUS_PER_TASK) {
            for (task <- tasksetManager.resourceOffer(execId, host, maxLocality)) {
              tasks(i) += task
              val tid = task.taskId
              taskIdToTaskSetId(tid) = tasksetManager.taskSet.id
              taskIdToExecutorId(tid) = execId
              activeExecutorIds += execId
              executorsByHost(host) += execId
              availableCpus(i) -= CPUS_PER_TASK
              assert(availableCpus(i) > 0)
              launchedTask = true
            }
          }
        }
      } while (launchedTask)
    }
    if (tasks.size > 0) {
      hasLaunchedTask = true
    }

    return tasks

  }

  def executorAdded(execId: String, host: String) {
    dagScheduler.executorAdded(execId, host)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  def isExecutorAlive(execId: String): Boolean = synchronized {
    activeExecutorIds.contains(execId)
  }
}
