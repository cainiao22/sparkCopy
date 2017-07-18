package org.apache.spark.scheduler

/**
 *  "FAIR" and "FIFO" determines which policy is used
 *    to order tasks amongst a Schedulable's sub-queues
 *  "NONE" is used when the a Schedulable has no sub-queues.
 */
object SchedulingMode extends Enumeration {

  type SchedulingMode = Value

  val FIFO, FAIR, NONE = Value
}
