package org.apache.spark.deploy

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import akka.actor._
import akka.pattern.ask
import akka.actor.{ActorSelection, Actor}
import akka.remote.RemotingLifecycleEvent
import org.apache.spark.deploy.master.{DriverState, Master}
import org.apache.spark.util.AkkaUtils
import org.apache.spark.{SparkConf, Logging}

import scala.concurrent.Await

/**
 * Proxy that relays messages to the driver.
 * 这个应该是给standalone用的，所以不会接受MasterChanged事件
 */
private class ClientActor(driverArgs: ClientArguments, conf: SparkConf) extends Actor with Logging {

  var masterActor:ActorSelection = _
  val timeout = AkkaUtils.askTimeout(conf)


  override def preStart(): Unit = {
    masterActor = context.actorSelection(Master.toAkkaUrl(driverArgs.master))

    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    println(s"Sending ${driverArgs.cmd} command to ${driverArgs.master}")

    driverArgs.cmd match {
      case "launch" =>
        // TODO: We could add an env variable here and intercept it in `sc.addJar` that would
        //       truncate filesystem paths similar to what YARN does. For now, we just require
        //       people call `addJar` assuming the jar is in the same directory.
        val env = Map[String, String]()
        System.getenv().foreach{case (k, v) => env(k) = v}

        val mainClass = "org.apache.spark.deploy.worker.DriverWrapper"

        val classPathConf = "spark.driver.extraClassPath"
        val classPathEntries = sys.props.get(classPathConf).toSeq.flatMap{cp =>
          cp.split(File.pathSeparator)
        }

        val libraryPathConf = "spark.driver.extraLibraryPath"
        val libraryPathEntries = sys.props.get(libraryPathConf).toSeq.flatMap{cp =>
          cp.split(File.pathSeparator)
        }

        val javaOptionsConf = "spark.driver.extraJavaOptions"
        val javaOpts = sys.props.get(javaOptionsConf)

        val command = new Command(mainClass, Seq("{{WORKER_URL}}", driverArgs.mainClass) ++
          driverArgs.driverOptions, env, classPathEntries, libraryPathEntries, javaOpts)

        val driverDescription = new DriverDescription(
        driverArgs.jarUrl,
        driverArgs.memory,
        driverArgs.cores,
        driverArgs.supervise,
        command
        )

        masterActor ! RequestSubmitDriver(driverDescription)

      case "kill" =>
        val driverId = driverArgs.driverId
        val killFuture = masterActor ! RequestKillDriver(driverId)
    }

  }

  def pollAndReportStatus(driverId:String): Unit ={
    println(s"...waiting before polling master driver state")
    Thread.sleep(5000)
    println("...polling master for driver state")
    val statusFuture = (masterActor ? RequestDriverStatus(driverId))(timeout)
      .mapTo[DriverStatusResponse]
    val statusResponse = Await.result(statusFuture, timeout)

    statusResponse.found match {
      case false =>
        println(s"Error: cluster master did not recognize $driverId")
        System.exit(-1)
      case true =>
        println(s"State of $driverId is ${statusResponse.state.get}")
        // Worker node, if present
        (statusResponse.workerId, statusResponse.workerHostPort, statusResponse.state) match {
          case (Some(id), Some(hostPort), Some(DriverState.RUNNING)) =>
            println(s"Driver running on $hostPort ($id)")
          case _ =>
        }
        // Exception, if present
        statusResponse.exception.map { e =>
          println(s"Exception from cluster was: $e")
          System.exit(-1)
        }
        System.exit(0)
    }
  }

  override def receive: Receive = {
    case SubmitDriverResponse(success, driverId, message) =>
      println(message)
      if(success) pollAndReportStatus(driverId.get) else System.exit(-1)
  }
}

class Client {

}
