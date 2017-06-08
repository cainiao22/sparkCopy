package org.apache.spark.deploy.master

import java.io.ObjectInputStream

import akka.actor.ActorRef
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] class WorkerInfo(
     val id:String,
     val host:String,
     val port:Int,
     val cores:Int,
     val memory:Int,
     val actor:ActorRef,
     val webUiPort:Int,
     val publicAddress:String
       ) extends Serializable {

  Utils.checkHost(host, "Expected hostname")
  assert(port > 0)

  @transient var executors:mutable.HashMap[String, ExecutorInfo] = _ // executorId => info
  @transient var drivers:mutable.HashMap[String, DriverInfo] = _ //driverId => driverInfo
  @transient var state:WorkerState.Value = _
  @transient var coresUsed:Int = _
  @transient var memoryUsed:Int = _

  @transient var lastHeartbeat:Long = _

  init()

  def coresFree:Int = cores - coresUsed
  def memoryFree:Int = memory - memoryUsed

  private def readObject(in:ObjectInputStream): Unit ={
    //只读取非static和非transient字段，且只能在readObject中调用
    in.defaultReadObject()
    init()
  }

  private def init(): Unit ={
    executors = new mutable.HashMap
    drivers = new mutable.HashMap
    state = WorkerState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

}
