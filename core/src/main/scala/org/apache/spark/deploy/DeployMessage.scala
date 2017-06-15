package org.apache.spark.deploy

import org.apache.spark.deploy.ExecutorState.ExecutorState
import org.apache.spark.deploy.master.DriverState._
import org.apache.spark.util.Utils

import scala.collection.immutable.List

/**
 * Created by Administrator on 2017/6/1.
 */
private[deploy] sealed trait DeployMessage extends Serializable

case class RegisterWorker(
                         id:String,
                         host:String,
                         port:Int,
                         cores:Int,
                         memory:Int,
                         webUiPort:Int,
                         publicAddress:String
                           ) extends DeployMessage {
  Utils.checkHost(host, "Required hostname")
  assert(port > 0)
}

case class DriverStateChanged(
                               driverId: String,
                               state: DriverState,
                               exception: Option[Exception]) extends DeployMessage

//worker to master

case class WorkerSchedulerStateResponse(id: String, executors: List[ExecutorDescription],
                                        driverIds: Seq[String])



// DriverClient <-> Master

case class RequestSubmitDriver(driverDescription: DriverDescription) extends DeployMessage


case class RequestKillDriver(driverId:String) extends DeployMessage

// Master to Worker & AppClient

case class MasterChanged(masterUrl:String, masterWebUiUrl:String)

case class MasterChangeAcknowledged(appId:String)

case class ExecutorUpdated(id:Int, state:ExecutorState, message:Option[String],
                            exitStatus:Option[Int])


case class Heartbeat(workerId: String) extends DeployMessage

//master to worker

case class RegisteredWorker(masterUrl:String, masterWebUiUrl:String) extends DeployMessage

case class RegisterWorkerFailed(message: String) extends DeployMessage

case class KillExecutor(masterUrl:String, appId:String, execId:Int) extends DeployMessage

case class LaunchExecutor(
                           masterUrl: String,
                           appId: String,
                           execId: Int,
                           appDesc: ApplicationDescription,
                           cores: Int,
                           memory: Int)
  extends DeployMessage

case class LaunchDriver(driverId: String, driverDesc: DriverDescription) extends DeployMessage

// Worker internal

case object WorkDirCleanup      // Sent to Worker actor periodically for cleaning up app folders

// AppClient to Master

case class RegisterApplication(appDescription: ApplicationDescription)
  extends DeployMessage

//master to AppClient

case class ApplicationRemoved(message: String)

// TODO(matei): replace hostPort with host
case class ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) {
  Utils.checkHostPort(hostPort, "Required hostport")
}



// Liveness checks in various places

case object SendHeartbeat