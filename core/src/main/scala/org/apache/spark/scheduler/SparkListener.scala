package org.apache.spark.scheduler


sealed trait SparkListenerEvent

/** An event used in the listener to shutdown the listener daemon thread. */
private[spark] case object SparkListenerShutdown extends SparkListenerEvent

/**
 * :: DeveloperApi ::
 * Interface for listening to events from the Spark scheduler. Note that this is an internal
 * interface which might change in different Spark releases.
 */
trait SparkListener {

}
