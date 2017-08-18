package org.apache.spark.executor

import java.nio.ByteBuffer

import akka.actor.Actor.Receive
import akka.actor.{Props, Actor, ActorSystem}
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.{SparkConf, Logging, SecurityManager}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Created by QDHL on 2017/8/8.
 */
private[spark] class CoarseGrainedExecutorBackend(driverUrl: String,
                                                  executorId: String,
                                                  hostPort: String,
                                                  cores: Int,
                                                  actorSystem: ActorSystem) extends Actor with ExecutorBackend with Logging {

  Utils.checkHostPort(hostPort, "Expected hostport")

  var executor:Executor = null

  override def preStart(): Unit ={
    logInfo(s"conneting to driver: $driverUrl")
    driver
  }

  override def receive: Receive = ???

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer): Unit = ???
}

private[spark] object CoarseGrainedExecutorBackend {

  def run(driverUrl: String, executorId: String, hostname: String, cores: Int,
          workerUrl: Option[String]): Unit = {

    SparkHadoopUtil.get.runAsSparkUser(() => {
      Utils.checkHost(hostname)
      val conf = new SparkConf()
      // Create a new ActorSystem to run the backend, because we can't create a
      // SparkEnv / Executor before getting started with all our system properties, etc
      val (actorSystem, boundPort) = AkkaUtils.createActorSystem("sparkExecutor", hostname, 0,
        conf, new SecurityManager(conf))
      val sparkHostPort = hostname + ":" + boundPort
      actorSystem.actorOf(
        //这里就是反射
        Props(classOf[CoarseGrainedExecutorBackend], driverUrl, executorId, sparkHostPort, cores, actorSystem),
        name = "Executor"
      )

      workerUrl.foreach{
        url =>
          actorSystem.actorOf(Props(classOf[WorkerWatcher], url), name = "WorkerWatcher")
      }
      actorSystem.awaitTermination()
    })

  }


  def main(args: Array[String]) {
    args.length match {
      case x if x < 4 =>
        System.err.println(
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          "Usage: CoarseGrainedExecutorBackend <driverUrl> <executorId> <hostname> " +
            "<cores> [<workerUrl>]")
        System.exit(1)
      case 4 =>
        run(args(0), args(1), args(2), args(3).toInt, None)
      case x if x > 4 =>
        run(args(0), args(1), args(2), args(3).toInt, Some(args(4)))
    }
  }
}
