package org.apache.spark.scheduler

import org.apache.spark.SparkContext

/**
 * Created by QDHL on 2017/7/18.
 */
private[spark] class DAGScheduler(private[scheduler] val sc:SparkContext,
                                  private[scheduler] val taskScheduler: TaskScheduler
                                   ) {



  def this(sc:SparkContext) = this(sc, sc.taskScheduler)

}
