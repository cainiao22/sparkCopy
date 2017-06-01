package org.apache.spark.deploy

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] case class DriverDescription(
     val jarUrl:String,
     val mem:Int,
     val cores:Int,
     val supervise:Boolean,
     val command: Command
       ) extends Serializable {

}
