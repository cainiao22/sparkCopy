package org.apache.spark.util


/**
 * An interface to represent clocks, so that they can be mocked out in unit tests.
 */
private[spark] trait Clock {
  def getTime():Long
}

private[spark] object SystemClock extends Clock {
  override def getTime() = System.currentTimeMillis()
}
