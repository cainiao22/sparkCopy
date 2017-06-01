package org.apache.spark.deploy.master

import akka.actor.ActorRef
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] class WorkerInfo(
     val id:String,
     val host:String,
     val port:Int,
     val cores:Int,
     val memory:Int,
     val actor:ActorRef,
     val webUiPort:Int,
     val publicAddress:String
       ) extends Serializable {

  Utils.checkHost(host, "Expected hostname")
  assert(port > 0)

  var executors:mutable.HashMap[String, ]
}
