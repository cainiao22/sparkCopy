package org.apache.spark.scheduler

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import scala.collection.JavaConversions._
import org.apache.spark.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

import scala.collection.mutable.ArrayBuffer

/**
 * An Schedulable entity that represent collection of Pools or TaskSetManagers
 */
private[spark] class Pool(val poolName:String,
                          val schedulingMode: SchedulingMode,
                           initMinShare:Int,
                           initWeight:Int) extends Schedulable with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]()
  var weight = initWeight
  var minShare = initMinShare
  var runningTasks = 0
  var priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  var name = poolName
  var parent:Pool = null

  var teskSchedulingAlgorithm = schedulingMode match {
    case SchedulingMode.FAIR =>
      new FairSchedulingAlgorithm
    case SchedulingMode.FIFO =>
      new FIFOSchedulingAlgorithm
  }

  override def minShar: Int = ???

  override def addSchedulable(schedulable: Schedulable): Unit = {
    require(schedulable != null)
    schedulableQueue.add(schedulable)
    schedulableNameToSchedulable.put(schedulable.name, schedulable)
    schedulable.parent = this
  }

  override def removeScheduleable(schedulable: Schedulable): Unit = {
    schedulableQueue.remove(schedulable)
    schedulableNameToSchedulable.remove(schedulable.name)
  }

  //判断某个任务是否可以启动推测执行
  override def checkSpeculatableTasks(): Boolean = {
    var shouldReceive = false
    for(scheduleable <- schedulableQueue){
      shouldReceive |= scheduleable.checkSpeculatableTasks()
    }
    shouldReceive
  }

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]()
    val sortedSchedulableQueue
      = schedulableQueue.toSeq.sortWith(teskSchedulingAlgorithm.comparator)
    for(schedulable <- sortedSchedulableQueue){
      sortedTaskSetQueue ++= schedulable.getSortedTaskSetQueue()
    }
    sortedTaskSetQueue
  }

  //这里需要遍历整个树结构
  override def getSchedulableByName(name: String): Schedulable = {
    if(schedulableNameToSchedulable.contains(name)){
      return schedulableNameToSchedulable.get(name)
    }
    for(schedulable <- schedulableQueue){
      val sched = schedulable.getSchedulableByName(name)
      if(sched != null){
        return sched
      }
    }

    return null
  }

  override def executorLost(executorId: String, host: String): Unit = {
    schedulableQueue.foreach(_.executorLost(executorId, host))
  }
}
