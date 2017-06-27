package org.apache.spark.util

import org.apache.spark.Logging

import scala.collection.mutable

/**
 * A wrapper of TimeStampedHashMap that ensures the values are weakly referenced and timestamped.
 *
 * If the value is garbage collected and the weak reference is null, get() will return a
 * non-existent value. These entries are removed from the map periodically (every N inserts), as
 * their values are no longer strongly reachable. Further, key-value pairs whose timestamps are
 * older than a particular threshold can be removed using the clearOldValues method.
 *
 * TimeStampedWeakValueHashMap exposes a scala.collection.mutable.Map interface, which allows it
 * to be a drop-in replacement for Scala HashMaps. Internally, it uses a Java ConcurrentHashMap,
 * so all operations on this HashMap are thread-safe.
 *
 * @param updateTimeStampOnGet Whether timestamp of a pair will be updated when it is accessed.
 */
private[spark] class TimeStampedWeakValueHashMap[A, B](updateTimeStampOnGet:Boolean = false)
  extends mutable.Map[A, B] with Logging {

  private val internalMap = new

}
