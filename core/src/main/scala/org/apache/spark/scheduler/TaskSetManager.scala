package org.apache.spark.scheduler

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.util.{SystemClock, Clock}

import scala.collection.immutable.{HashSet, HashMap}
import scala.collection.mutable.ArrayBuffer

/**
 * Schedules the tasks within a single TaskSet in the TaskSchedulerImpl. This class keeps track of
 * each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * TaskScheduler (e.g. its event handlers). It should not be called from other threads.
 *
 * @param sched           the TaskSchedulerImpl associated with the TaskSetManager
 * @param taskSet         the TaskSet to manage scheduling for
 * @param maxTaskFailures if any particular task fails more than this number of times, the entire
 *                        task set will be aborted
 */
private[spark] class TaskSetManager(sched: TaskSchedulerImpl,
                                    val taskSet: TaskSet,
                                    val maxTaskFailures: Int,
                                    val clock: Clock = SystemClock)
  extends Schedulable with Logging {

  val conf = sched.sc.conf

  /*
   * Sometimes if an executor is dead or in an otherwise invalid state, the driver
   * does not realize right away leading to repeated task failures. If enabled,
   * this temporarily prevents a task from re-launching on an executor where
   * it just failed.
   */
  private val EXECUTOR_TASK_BLACKLIST_TIMEOUT =
    conf.getLong("spark.scheduler.executorTaskBlacklistTime", 0L)

  // Quantile of tasks at which to start speculation
  val SPECULATION_QUANTILE = conf.getDouble("spark.speculation.quantile", 0.75)
  val SPECULATION_MULTIPLIER = conf.getDouble("spark.speculation.multiplier", 1.5)

  val env = SparkEnv.get
  val serializer = env.closureSerializer.newInstance()

  val tasks = taskSet.tasks
  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)
  val successful = new Array[Boolean](numTasks)
  private val numFailures = new Array[Int](numTasks)
  // key is taskId, value is a Map of executor id to when it failed
  val failedExecutors = new HashMap[Int, Map[String, Long]]()
  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  var tasksSuccessful = 0

  var weight = 1
  var minShare = 0
  var priority = taskSet.priority
  var stageId = taskSet.stageId
  var name = "taskSet_" + taskSet.stageId.toString
  var parent: Pool = null

  val runningTaskSet = new HashSet[Long]
  override def runningTasks = runningTaskSet.size

  // True once no more tasks should be launched for this task set manager. TaskSetManagers enter
  // the zombie state once at least one attempt of each task has completed successfully, or if the
  // task set is aborted (for example, because it was killed).  TaskSetManagers remain in the zombie
  // state until all tasks have finished running; we keep TaskSetManagers that are in the zombie
  // state in order to continue to track and account for the running tasks.
  // TODO: We should kill any running task attempts when the task set manager becomes a zombie.
  var isZombie = false

  // Set of pending tasks for each executor. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. They are also only cleaned up lazily;
  // when a task is launched, it remains in all the pending lists except
  // the one that it was launched from, but gets removed from them later.
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  //机架
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]


  // Set containing pending tasks with no locality preferences.
  val pendingTasksWithNoPrefs = new ArrayBuffer[Int]()

  // Set containing all pending tasks (also used as a stack, as above).
  val allPendingTasks = new ArrayBuffer[Int]()

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  val taskInfos = new HashMap[Long, TaskInfo]

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    conf.getLong("spark.logging.exceptionPrintInterval", 10000)



  def resourceOffer(execId:String, host:String, )
}
