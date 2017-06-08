package org.apache.spark.deploy.master

/**
 * Created by Administrator on 2017/6/8.
 */
private[spark] class ExecutorInfo(val id: Int,
                                  val application: ApplicationInfo,
                                  val worker: WorkerInfo,
                                  val cores: Int,
                                  val memory: Int) {

}
