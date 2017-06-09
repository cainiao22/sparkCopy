package org.apache.spark.deploy

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] case class DriverDescription(
     val jarUrl:String,
     val mem:Int,
     val cores:Int,
     val supervise:Boolean, //sparkSubmit参数中设置的，表示失败后是否重启
     val command: Command
       ) extends Serializable {

}
