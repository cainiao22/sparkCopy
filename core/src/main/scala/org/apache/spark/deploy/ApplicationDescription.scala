package org.apache.spark.deploy

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] class ApplicationDescription(
                                           val name:String,
                                           val maxCores:Option[Int],
                                           val memoryPerSlave:Int,
                                           val command:Command,
                                           val sparkHome:Option[String],
                                           val appUiUrl:String,
                                           val eventLogDir:Option[String] = None
                                             ) extends Serializable {

  val user = System.getProperty("user.name", "<unknown>")

  override def toString: String = "ApplicationDescription(" + name + ")"
}
