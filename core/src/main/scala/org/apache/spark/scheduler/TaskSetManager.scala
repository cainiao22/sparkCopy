package org.apache.spark.scheduler

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.util.{SystemClock, Clock}

import scala.collection.mutable.{HashMap, ArrayBuffer, HashSet}

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
  val ser = env.closureSerializer.newInstance()

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

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  val recentExceptions = HashMap[String, (Int, Long)]

  // Figure out the current map output tracker epoch and set it on all tasks
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  tasks.foreach(t => t.epoch = epoch)

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  // Figure out which locality levels we have in our TaskSet, so we can do delay scheduling
  val myLocalityLevels = computeValidLocalityLevels()
  val localityWaits = myLocalityLevels.map(getLocalityWait) // Time to wait at each level

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  var currentLocalityIndex = 0
  // Index of our current locality level in validLocalityLevels
  var lastLaunchTime = clock.getTime() // Time we last launched a task at this level

  private def addPendingTask(index: Int, readding: Boolean = false): Unit = {

    // Utility method that adds `index` to a list only if readding=false or it's not already there
    def addTo(list: ArrayBuffer[Int]): Unit = {
      if (!readding && !list.contains(index)) {
        list += index
      }
    }
    var haveAliveLocations = false
    for (loc <- tasks(index).preferredLocations) {
      for (execId <- loc.executorId) {
        if (sched.isExecutorAlive(execId)) {
          addTo(pendingTasksForExecutor.getOrElseUpdate(execId, new ArrayBuffer[Int]))
          haveAliveLocations = true
        }
      }

      if (sched.hasExecutorsAliveOnHost(loc.host)) {
        addTo(pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer[Int]))
        for (rack <- sched.getRackForHost(loc.host)) {
          addTo(pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer))
        }
      }
    }

    if (!haveAliveLocations) {
      // Even though the task might've had preferred locations, all of those hosts or executors
      // are dead; put it in the no-prefs list so we can schedule it elsewhere right away.
      addTo(pendingTasksWithNoPrefs)
    }

    if (!readding) {
      allPendingTasks += index // No point scanning this whole list to find the old task there
    }
  }

  /**
   * Is this re-execution of a failed task on an executor it already failed in before
   * EXECUTOR_TASK_BLACKLIST_TIMEOUT has elapsed ?
   */
  private def executorIsBlacklisted(execId: String, taskId: Int): Boolean = {
    if (failedExecutors.contains(taskId)) {
      val failed = failedExecutors(taskId)
      return failed.contains(execId) && clock.getTime() - failed.get(execId).get > EXECUTOR_TASK_BLACKLIST_TIMEOUT
    }
    false
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   */
  private def findTaskFromList(execId: String, list: ArrayBuffer[Int]): Option[Int] = {
    var indexOffset = list.size
    while (indexOffset > 0) {
      indexOffset -= 1
      val index = list(indexOffset)
      if (!executorIsBlacklisted(execId, index)) {
        list.remove(indexOffset)
        if (copiesRunning(index) == 0 && !successful(index)) {
          Some(index)
        }
      }
    }
    None
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   */
  private def findTask(execId: String, host: String, locality: TaskLocality.Value)
  : Option[(Int, TaskLocality.Value)] = {
    for (index <- findTaskFromList(execId, getPendingTasksForExecutor(execId))) {
      return Some((index, TaskLocality.PROCESS_LOCAL))
    }
    if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
      for (index <- findTaskFromList(host, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL))
      }
    }
    if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
      for {rack <- sched.getRackForHost(host)
           index <- findTaskFromList(execId, getPendingTasksForRack(rack))} {
        return Some((index, TaskLocality.RACK_LOCAL))
      }
    }

    for (index <- findTaskFromList(execId, pendingTasksWithNoPrefs)) {
      return Some((index, TaskLocality.PROCESS_LOCAL))
    }

    if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
      for (index <- findTaskFromList(execId, allPendingTasks)) {
        return Some((index, TaskLocality.ANY))
      }
    }

    return findSpeculativeTask(execId, host, locality)
  }

  /**
   * Return a speculative task for a given executor if any are available. The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
   */
  private def findSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
  : Option[(Int, TaskLocality.Value)] = {
    speculatableTasks.retain(index => !successful(index)) // Remove finished tasks from set

    def canRunOnHost(index: Int): Boolean = {
      !hasAttemptOnHost(index, host) && !executorIsBlacklisted(execId, index)
    }

    if (!speculatableTasks.isEmpty) {
      // Check for process-local or preference-less tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming
      for (index <- speculatableTasks if canRunOnHost(index)) {
        val prefs = tasks(index).preferredLocations
        val executors = prefs.flatMap(_.executorId)
        if (prefs.size == 0 || executors.contains(execId)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }

        // Check for node-local tasks
        if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            val hosts = tasks(index).preferredLocations.map(_.host)
            if (hosts.contains(host)) {
              speculatableTasks -= index;
              return Some((index, TaskLocality.NODE_LOCAL))
            }
          }
        }

        // Check for rack-local tasks
        if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            val racks = tasks(index).preferredLocations.map(_.host).map(sched.getRackForHost)
            if (racks.contains(sched.getRackForHost(host))) {
              speculatableTasks -= index
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }

        // Check for non-local tasks
        if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
          for (index <- speculatableTasks if canRunOnHost(index)) {
            speculatableTasks -= index
            return Some((index, TaskLocality.ANY))
          }
        }
      }

      None
    }
  }

  /** Check whether a task is currently running an attempt on a given host */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    taskAttempts(taskIndex).exists(_.host == host)
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())
  }

  /**
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   */
  def resourceOffer(execId: String,
                    host: String,
                    maxLocality: TaskLocality.TaskLocality): Option[TaskDescription] = {
    if (!isZombie) {
      val curTime = clock.getTime()
      var allowedLocality = getAllowedLocalityLevel(curTime)
      if (allowedLocality > maxLocality) {
        allowedLocality = maxLocality
      }

      findTask(execId, host,, allowedLocality) match {
        case Some((index, taskLocality)) =>
          // Found a task; do some bookkeeping and return a task description
          val task = tasks(index)
          val taskId = sched.nextTaskId
          // Figure out whether this should count as a preferred launch
          logInfo("Starting task %s:%d as TID %s on executor %s: %s (%s)".format(
            taskSet.id, index, taskId, execId, host, taskLocality))
          // Do various bookkeeping
          copiesRunning(index) += 1
          val info = new TaskInfo(taskId, index, curTime, execId, host, taskLocality)
          taskInfos(taskId) = info
          taskAttempts(index) = info :: taskAttempts(index)
          // Update our locality level for delay scheduling
          val currentLocalityIndex = getLocalityIndex(taskLocality)
          lastLaunchTime = curTime
          // Serialize and return the task
          val startTime = clock.getTime()
          // We rely on the DAGScheduler to catch non-serializable closures and RDDs, so in here
          // we assume the task can be serialized without exceptions.
          val serializedTask = Task.serializeWithDependencies(task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          val timeToken = clock.getTime() - startTime;
          addRunningTask(taskId)
          logInfo("Serialized task %s:%d as %d bytes in %d ms".format(
            taskSet.id, index, serializedTask.limit, timeToken))
          val taskName = "task %s:%d".format(taskSet.id, index)
          sched.dagScheduler.taskStarted(task, info)
          return Some(new TaskDescription(taskId, execId, taskName, index, serializedTask))
      }
    }
    None
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    while (curTime - lastLaunchTime > localityWaits(currentLocalityIndex)
      && currentLocalityIndex < myLocalityLevels.length - 1) {
      lastLaunchTime += localityWaits(currentLocalityIndex)
      currentLocalityIndex += 1
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    if (!pendingTasksForExecutor.isEmpty && getLocalityWait(TaskLocality.PROCESS_LOCAL) > 0) {
      levels += TaskLocality.PROCESS_LOCAL
    }
    if (!pendingTasksForHost.isEmpty && getLocalityWait(TaskLocality.NODE_LOCAL) > 0) {
      levels += TaskLocality.NODE_LOCAL
    }
    if (!pendingTasksForRack.isEmpty && getLocalityWait(TaskLocality.RACK_LOCAL) > 0) {
      levels += TaskLocality.RACK_LOCAL
    }
    levels += TaskLocality.ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }

  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    val defaultWait = conf.get("spark.locality.wait", "3000")
    level match {
      case TaskLocality.PROCESS_LOCAL =>
        conf.get("spark.locality.wait.process", defaultWait).toLong
      case TaskLocality.NODE_LOCAL =>
        conf.get("spark.locality.wait.node", defaultWait).toLong
      case TaskLocality.RACK_LOCAL =>
        conf.get("spark.locality.wait.rack", defaultWait).toLong
      case TaskLocality.ANY =>
        0L
    }
  }

  def addRunningTask(tid:Long): Unit ={
    if(runningTaskSet.add(tid) && parent != null){
      parent.increaseRunningTasks(1)
    }
  }
}
