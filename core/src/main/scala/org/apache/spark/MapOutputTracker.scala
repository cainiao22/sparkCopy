package org.apache.spark

/**
 * Class that keeps track of the location of the map output of
 * a stage. This is abstract because different versions of MapOutputTracker
 * (driver and worker) use different HashMap to store its metadata.
 */
private[spark] abstract class MapOutputTracker {

  /**
   * Incremented every time a fetch fails so that client nodes know to clear
   * their cache of map output locations if this happens.
   */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef

  def getEpoch: Long = epochLock.synchronized {
    return epoch
  }

}
