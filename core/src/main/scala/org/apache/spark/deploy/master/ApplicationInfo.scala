package org.apache.spark.deploy.master

import java.util.Date

import akka.actor.ActorRef
import org.apache.spark.deploy.ApplicationDescription

import scala.collection.mutable

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] class ApplicationInfo(
                                      val startTime: Long,
                                      val id: String,
                                      val desc: ApplicationDescription,
                                      val submitDate: Date,
                                      val driver: ActorRef,
                                      defaultCores: Int
                                      ) extends Serializable {


  @transient var state:ApplicationState.Value = _
  @transient var executors: mutable.HashMap[Int, ExecutorInfo] = _
  @transient var coresGranted:Int = _
  @transient var endTime:Long = _
  @transient var appSource:ApplicationSource = _

  @transient private var nextExecutorId:Int = _

  init()

  private def init(): Unit ={
    state = ApplicationState.RUNNING
    executors = new mutable.HashMap[Int, ExecutorInfo]()
    coresGranted = 0
    endTime = -1L
    appSource = new ApplicationSource(this)
    nextExecutorId = 0
  }

  private def newExecutorId(userID:Option[Int] = None):Int = {
    userID match {
      case Some(id) =>
        nextExecutorId = math.max(nextExecutorId, id + 1)
        id
      case None =>
        val id = nextExecutorId
        nextExecutorId = nextExecutorId + 1
        id
    }
  }


  def removeExecutor(exec:ExecutorInfo): Unit ={
    if(executors.contains(exec.id)){
      executors -= exec.id
      coresGranted -= exec.cores
    }
  }

  def markFinished(endState: ApplicationState.Value) = {
    this.state = endState
    this.endTime = System.currentTimeMillis()
  }
}
