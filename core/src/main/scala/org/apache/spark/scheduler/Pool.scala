package org.apache.spark.scheduler

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import org.apache.spark.Logging
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * An Schedulable entity that represent collection of Pools or TaskSetManagers
 */
private[spark] class Pool(val poolName:String,
                          val schedulingMode: SchedulingMode,
                           initMinShare:Int,
                           initWeight:Int) extends Schedulable with Logging {

  val schedulableQueue = new ConcurrentLinkedQueue[Schedulable]()
  val schedulableNameToSchedulable = new ConcurrentHashMap[String, Schedulable]()
  var weight = initWeight
  var minShare = initMinShare
  var runningTasks = 0
  var priority = 0

  // A pool's stage id is used to break the tie in scheduling.
  var stageId = -1
  var name = poolName
  var parent:Pool = null

}
