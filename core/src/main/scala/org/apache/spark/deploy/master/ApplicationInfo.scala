package org.apache.spark.deploy.master

import java.util.Date

import akka.actor.ActorRef
import org.apache.spark.deploy.ApplicationDescription

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] class ApplicationInfo(
                                      val startTime: Long,
                                      val id: String,
                                      val desc: ApplicationDescription,
                                      val submitDate: Date,
                                      val driver: ActorRef,
                                      defaultCores: Int
                                      ) extends Serializable {

}
