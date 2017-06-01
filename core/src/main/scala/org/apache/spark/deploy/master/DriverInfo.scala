package org.apache.spark.deploy.master

import java.util.Date

import org.apache.spark.deploy.DriverDescription

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] class DriverInfo(
                                 val startTime: Long,
                                 val id: String,
                                 val desc: DriverDescription,
                                 val submitDate: Date
                                 ) extends Serializable {

}
