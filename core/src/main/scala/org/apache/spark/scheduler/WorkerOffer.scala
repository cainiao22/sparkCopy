package org.apache.spark.scheduler

/**
 * Represents free resources available on an executor.
 */
private [spark]
case class WorkerOffer(executorId:String, host:String, cores:Int)
