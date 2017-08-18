package org.apache.spark.scheduler.cluster

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.remote.RemotingLifecycleEvent
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{ReviveOffers, LaunchTask}
import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.scheduler.{WorkerOffer, TaskDescription, SchedulerBackend, TaskSchedulerImpl}
import org.apache.spark.util.{SerializableBuffer, AkkaUtils}

import scala.collection.mutable.{HashMap, ArrayBuffer}
import scala.concurrent.duration._

/**
 * A scheduler backend that waits for coarse grained executors to connect to it through Akka.
 * This backend holds onto each executor for the duration of the Spark job rather than relinquishing
 * executors whenever a task is done and asking the scheduler to launch a new executor for
 * each new task. Executors may be launched in a variety of ways, such as Mesos tasks for the
 * coarse-grained Mesos mode or standalone processes for Spark's standalone deploy mode
 * (spark.deploy.*).
 */
private[spark] class CoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl,
                                                   actorSystem: ActorSystem
                                                    ) extends SchedulerBackend with Logging {

  var totalCoreCount = new AtomicInteger(0)
  val conf = scheduler.conf
  private val timeout = AkkaUtils.askTimeout(conf)
  private val akkaFrameSize = AkkaUtils.maxFrameSizeBytes(conf)

  class DriverActor(sparkProperties: Seq[(String, String)]) extends Actor {
    private val executorActor = new HashMap[String, ActorRef]
    //executorId -> host
    private val executorHost = new HashMap[String, String]
    private val freeCores = new HashMap[String, Int]

    override def preStart = {
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      actorSystem.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

      // Periodically revive offers to allow delay scheduling to work
      val receiveInterval = conf.getLong("spark.scheduler.revive.interval", 1000)
      import CoarseGrainedClusterMessages._
      actorSystem.scheduler.schedule(0.millis, receiveInterval.millis, self, ReviveOffers)

    }

    override def receive = {

      case ReviveOffers =>
        makeOffers()

    }

    def makeOffers(): Unit = {
      launchTask(scheduler.resourceOffers(
        executorHost.toArray.map{case (id, host) => new WorkerOffer(id, host, freeCores(id))}))
    }

    def launchTask(tasks: Seq[Seq[TaskDescription]]): Unit = {
      for(task <- tasks.flatten){
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val serializedTask = ser.serialize(task)
        if(serializedTask.limit() > akkaFrameSize - AkkaUtils.reservedSizeBytes){
          val taskSetId = scheduler.taskIdToTaskSetId(task.taskId)
          scheduler.activeTaskSets.get(taskSetId).foreach{taskSet =>
            try {
              var msg = "Serialized task %s:%d was %d bytes which " +
                "exceeds spark.akka.frameSize (%d bytes). " +
                "Consider using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize)
              taskSet.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }else{
          freeCores(task.executorId) -= scheduler.CPUS_PER_TASK
          executorActor(task.executorId) ! LaunchTask(new SerializableBuffer(serializedTask))
        }
      }
    }
  }

  var driverActor: ActorRef = null


  override def start(): Unit = {
    val properties = new ArrayBuffer[(String, String)]
    for ((k, v) <- scheduler.sc.getConf.getAll() if k.startsWith("spark.")) {
      properties.+=((k, v))
    }

    driverActor = actorSystem.actorOf(
      Props(new DriverActor(properties)), name = CoarseGrainedSchedulerBackend.ACTOR_NAME)
  }

}

private[spark] object CoarseGrainedSchedulerBackend {
  val ACTOR_NAME = "CoarseGrainedScheduler"
}
