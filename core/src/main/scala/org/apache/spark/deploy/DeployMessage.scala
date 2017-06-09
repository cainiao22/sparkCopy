package org.apache.spark.deploy

import org.apache.spark.deploy.ExecutorState.ExecutorState

/**
 * Created by Administrator on 2017/6/1.
 */
private[deploy] sealed trait DeployMessage extends Serializable


// DriverClient <-> Master

case class RequestSubmitDriver(driverDescription: DriverDescription) extends DeployMessage


case class RequestKillDriver(driverId:String) extends DeployMessage

// Master to Worker & AppClient

case class MasterChanged(masterUrl:String, masterWebUiUrl:String)

case class ExecutorUpdated(id:Int, state:ExecutorState, message:Option[String],
                            exitStatus:Option[Int])