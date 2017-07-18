package org.apache.spark.scheduler

import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

import scala.collection.mutable.ArrayBuffer

/**
 * An interface for schedulable entities.
 * there are two type of Schedulable entities(Pools and TaskSetManagers)
 */
private[spark] trait Schedulable {
  var parent:Pool

  // child queues
  def schedulableQueue:ConcurrentLinkedQueue[Schedulable]
  def schedulingMode:SchedulingMode
  def weight:Int
  def minShar:Int
  def runningTasks:Int
  def priority:Int
  def stageId:Int
  def name:String

  def addSchedulable(schedulable: Schedulable):Unit
  def removeScheduleable(schedulable: Schedulable):Unit
  def getSchedulableByName(name:String):Schedulable
  def executorLost(executorId:String, host:String):Unit
  //判断某个任务是否可以启动推测执行
  def checkSpeculatableTasks():Boolean
  def getSortedTaskSetQueue():ArrayBuffer[TaskSetManager]
}
