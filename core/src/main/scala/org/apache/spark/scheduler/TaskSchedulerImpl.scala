package org.apache.spark.scheduler

import java.util.Timer
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark._

import scala.collection.mutable

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
  val nextTaskId = new AtomicLong(0)

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


}
