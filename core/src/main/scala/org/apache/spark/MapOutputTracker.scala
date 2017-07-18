package org.apache.spark

/**
 * Class that keeps track of the location of the map output of
 * a stage. This is abstract because different versions of MapOutputTracker
 * (driver and worker) use different HashMap to store its metadata.
 */
private[spark] abstract class MapOutputTracker {

}
