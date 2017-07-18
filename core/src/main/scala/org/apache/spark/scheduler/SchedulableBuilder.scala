package org.apache.spark.scheduler

import java.util.Properties

/**
 * An interface to build Schedulable tree
 * buildPools: build the tree nodes(pools)
 * addTaskSetManager: build the leaf nodes(TaskSetManagers)
 */
private[spark] trait SchedulableBuilder {
  def rootPool: Pool

  def buildPools()

  def addTaskSetManager(manager: Schedulable, prop: Properties)
}

private[spark] class FIFOSchedulableBuilder(val rootPool:Pool) extends SchedulableBuilder {

  override def buildPools(): Unit = {

  }

  override def addTaskSetManager(manager: Schedulable, prop: Properties): Unit = {
    rootPool.add
  }
}
