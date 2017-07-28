package org.apache.spark.scheduler

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * Low-level task scheduler interface, currently implemented exclusively by TaskSchedulerImpl.
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedulers tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
 */
private[spark] trait TaskScheduler {

  def rootPool:Pool

  def schedulingMode:SchedulingMode

  def start():Unit
}
