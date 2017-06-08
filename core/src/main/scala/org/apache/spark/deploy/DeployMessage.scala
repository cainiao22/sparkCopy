package org.apache.spark.deploy

/**
 * Created by Administrator on 2017/6/1.
 */
private[deploy] sealed trait DeployMessage extends Serializable


// DriverClient <-> Master

case class RequestSubmitDriver(driverDescription: DriverDescription) extends DeployMessage


case class RequestKillDriver(driverId:String) extends DeployMessage

// Master to Worker & AppClient

case class MasterChanged(masterUrl:String, masterWebUiUrl:String)