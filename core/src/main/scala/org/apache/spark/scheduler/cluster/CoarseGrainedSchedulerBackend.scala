package org.apache.spark.scheduler.cluster

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.remote.RemotingLifecycleEvent
import org.apache.spark.Logging
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessage.ReviveOffers
import org.apache.spark.scheduler.{TaskDescription, SchedulerBackend, TaskSchedulerImpl}
import org.apache.spark.util.AkkaUtils

import scala.collection.mutable.ArrayBuffer
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

    override def preStart = {
      // Listen for remote client disconnection events, since they don't go through Akka's watch()
      actorSystem.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

      // Periodically revive offers to allow delay scheduling to work
      val receiveInterval = conf.getLong("spark.scheduler.revive.interval", 1000)
      import CoarseGrainedClusterMessage._
      actorSystem.scheduler.schedule(0.millis, receiveInterval.millis, self, ReviveOffers)

    }

    override def receive ={

      case ReviveOffers =>


    }

    def makeOffers(): Unit ={

    }

    def launchTask(tasks:Seq[Seq[TaskDescription]]): Unit ={

    }
  }

  var driverActor:ActorRef = null



  override def start(): Unit ={
    val properties = new ArrayBuffer[(String, String)]
    for((k, v) <- scheduler.sc.getConf.getAll() if k.startsWith("spark.")){
      properties.+=((k, v))
    }

    driverActor = actorSystem.actorOf(
      Props(new DriverActor(properties)), name = CoarseGrainedSchedulerBackend.ACTOR_NAME)
  }

}

private[spark] object CoarseGrainedSchedulerBackend {
  val ACTOR_NAME = "CoarseGrainedScheduler"
}
