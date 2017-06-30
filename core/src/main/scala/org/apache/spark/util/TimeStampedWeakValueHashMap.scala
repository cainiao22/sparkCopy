package org.apache.spark.util

import java.lang.ref.WeakReference
import java.util.concurrent.atomic.AtomicInteger

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
private[spark] class TimeStampedWeakValueHashMap[A, B](updateTimeStampOnGet: Boolean = false)
  extends mutable.Map[A, B] with Logging {

  import TimeStampedWeakValueHashMap._

  private val internalMap = new TimeStampedHashMap[A, WeakReference[B]](updateTimeStampOnGet)
  private val insertCount = new AtomicInteger(0)

  /** Return a map consisting only of entries whose values are still strongly reachable. (返回未被回收的数据) */
  private def nonNullReferenceMap = internalMap.filter { case (_, ref) => ref.get() != null }

  def get(key: A): Option[B] = internalMap.get(key)

  def iterator: Iterator[(A, B)] = nonNullReferenceMap.iterator

  override def +[B1 >: B](kv: (A, B1)): mutable.Map[A, B1] = {
    val newMap = new TimeStampedWeakValueHashMap[A, B1]()
    val oldMap = nonNullReferenceMap.asInstanceOf[mutable.Map[A, WeakReference[B1]]]
    newMap.internalMap.putAll(oldMap.toMap)
    newMap.internalMap += kv
    newMap
  }

  override def -(key: A): mutable.Map[A, B] = {
    val newMap = new TimeStampedWeakValueHashMap[A, B]()
    newMap.internalMap.putAll(nonNullReferenceMap.toMap)
    newMap.internalMap -= key
    newMap
  }

  override def +=(kv: (A, B)): this.type = {
    this.internalMap += kv
    if (insertCount.incrementAndGet() % CLEAR_NULL_VALUES_INTERVAL == 0) {
      clearNullValues
    }
    this
  }

  override def -=(key: A): this.type = {
    internalMap -= key
    this
  }

  override def update(key: A, value: B) = this += ((key, value))

  override def apply(key: A): B = internalMap.apply(key)

  override def filter(p: ((A, B)) => Boolean): mutable.Map[A, B] = nonNullReferenceMap.filter(p)

  override def empty: mutable.Map[A, B] = new TimeStampedWeakValueHashMap[A, B]()

  override def size: Int = internalMap.size()

  override def foreach[U](f: ((A, B)) => U) = nonNullReferenceMap.foreach(f)

  def putIfAbsent(key: A, value: B): Option[B] = internalMap.putIfAbsent(key, value)

  def toMap: Map[A, B] = iterator.toMap


  def clearOldValues(threshTime: Long) = internalMap.clearOldValues(threshTime)

  /** Remove entries with values that are no longer strongly reachable. */
  def clearNullValues(): Unit = {
    val it = internalMap.getEntrySet.iterator()
    while (it.hasNext) {
      val entry = it.next()
      if (entry.getValue.value.get() == null) {
        logDebug("Removing key " + entry.getKey + " because it is no longer strongly reachable.")
        it.remove()
      }
    }
  }

}


private object TimeStampedWeakValueHashMap {
  // Number of inserts after which entries with null references are removed
  val CLEAR_NULL_VALUES_INTERVAL = 100

  /* Implicit conversion methods to WeakReferences. */
  implicit def toWeakReference[V](v: V): WeakReference[V] = new WeakReference[V](v)

  implicit def toWeakReferenceTuple[K, V](kv: (K, V)): (K, WeakReference[V]) = {
    kv match {
      case (k, v) => (k, toWeakReference(v))
    }
  }

  implicit def toWeakReferenceFunction[K, V, R](p: ((K, V)) => R): ((K, WeakReference[V])) => R = {
    (kv: (K, WeakReference[V])) => p(kv)
  }

  /* Implicit conversion methods from WeakReferences. */

  implicit def fromWeakReference[V](ref: WeakReference[V]): V = ref.get

  implicit def fromWeakReferenceOption[V](v: Option[WeakReference[V]]): Option[V] = {
    v match {
      case Some(ref) => Option(fromWeakReference(ref))
      case None => None
    }
  }

  implicit def fromWeakReferenceTuple[K, V](kv: (K, WeakReference[V])): (K, V) = {
    kv match {
      case (k, v) => (k, fromWeakReference(v))
    }
  }

  implicit def fromWeakReferenceIterator[K, V](
                                                it: Iterator[(K, WeakReference[V])]
                                                ): Iterator[(K, V)] = {
    it.map(fromWeakReferenceTuple)
  }

  implicit def fromWeakReferenceMap[K, V](
                                           map: mutable.Map[K, WeakReference[V]]): mutable.Map[K, V] = {

    //代表这里是一个可变参数
    mutable.Map(map.mapValues(fromWeakReference).toSeq: _*)
  }
}