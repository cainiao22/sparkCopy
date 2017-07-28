package org.apache.spark.scheduler

/**
 * A backend interface for scheduling systems that allows plugging in different ones under
 * TaskSchedulerImpl. We assume a Mesos-like model where the application gets resource offers as
 * machines become available and can launch tasks on them.
 */
private[spark] trait SchedulerBackend {
  def start():Unit
  /*def stop():Unit
  def reviveOffers():Unit
  def defaultParallelism():Int

  def killTask(taskId: Long, executorId: String, interruptThread: Boolean): Unit =
    throw new UnsupportedOperationException*/

}
