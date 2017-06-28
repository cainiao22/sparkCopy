package org.apache.spark.util

import java.util
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.Logging

import scala.collection.{JavaConversions, mutable}

private[spark] case class TimeStampedValue[V](value: V, timestamp: Long)

/**
 * This is a custom implementation of scala.collection.mutable.Map which stores the insertion
 * timestamp along with each key-value pair. If specified, the timestamp of each pair can be
 * updated every time it is accessed. Key-value pairs whose timestamp are older than a particular
 * threshold time can then be removed using the clearOldValues method. This is intended to
 * be a drop-in replacement of scala.collection.mutable.HashMap.
 *
 * @param updateTimeStampOnGet Whether timestamp of a pair will be updated when it is accessed
 */
private[spark] class TimeStampedHashMap[A, B](updateTimeStampOnGet: Boolean = false)
  extends mutable.Map[A, B] with Logging {

  private val internalMap = new ConcurrentHashMap[A, TimeStampedValue[B]]()

  def get(key: A): Option[B] = {
    val value = internalMap.get(key)
    if (value != null && updateTimeStampOnGet) {
      internalMap.replace(key, value, TimeStampedValue(value.value, currentTime))
    }
    Option(value).map(_.value)
  }

  def iterator: Iterator[(A, B)] = {
    val jIterator = getEntrySet.iterator
    JavaConversions.asScalaIterator(jIterator).map(kv => (kv.getKey, kv.getValue.value))
  }

  def getEntrySet: util.Set[Entry[A, TimeStampedValue[B]]] = internalMap.entrySet()

  /**
   * 表示：A和B为T上界
     T <: A with B 子类

      表示：A和B为T下界
     T >: A with B 超类

      表示：同时拥有上界和下界，并且A为下界，B为上界，A为B的子类，顺序不能颠倒。
     T >: A <: B

      表示：类型变量界定，即同时满足AT这种隐式值和BT这种隐式值
     T:A:B

     表示：视图界定，即同时能够满足隐式转换的A和隐式转换的B
     T <% A <% B
   */
  override def +[B1 >: B](kv: (A, B1)): mutable.Map[A, B1] = {
    val newMap = new TimeStampedHashMap[A, B1]()
    val oldMap = this.internalMap.asInstanceOf[ConcurrentHashMap[A, TimeStampedValue[B1]]]
    newMap.internalMap.putAll(oldMap)
    kv match {
      case (k, v) =>
        newMap.internalMap.put(k, new TimeStampedValue(v, currentTime))
    }
    newMap
  }

  override def -(key:A):mutable.Map[A, B] = {
    val newMap = new TimeStampedHashMap[A, B]()
    newMap.internalMap.putAll(this.internalMap)
    newMap.internalMap.remove(key)
    newMap
  }

  override def += (kv:(A, B)):this.type = {
    kv match {case (a, b) => internalMap.put(a, TimeStampedValue(b, currentTime))}
    this
  }

  override def -= (key:A):this.type = {
    internalMap.remove(key)
    this
  }

  override def update(key:A, value:B):this.type = {
    this += ((key, value))
  }

  override def apply(key:A):B = {
    get(key).getOrElse(throw new NoSuchElementException)
  }

  override def filter(p:((A, B)) => Boolean):mutable.Map[A, B] = {
    JavaConversions.mapAsScalaConcurrentMap(internalMap)
      .map{case (k, TimeStampedValue(v)) => (k, v)}.filter(p)
  }

  override def empty:mutable.Map[A, B] = new TimeStampedHashMap[A, B]()

  override def size():Int = internalMap.size()

  override def foreach[U](f:((A, B)) => U) {
    val it = getEntrySet.iterator()
    while(it.hasNext){
      val entry = it.next()
      val kv = (entry.getKey, entry.getValue.value)
      f(kv)
    }
  }

  def putIfAbsent(key:A, value:B):Option[B] = {
    val pre = internalMap.putIfAbsent(key, TimeStampedValue(value, currentTime))
    Option(pre).map(_.value)
  }

  def putAll(map:Map[A, B]): Unit ={
    map.foreach{case (k, v) => update(k, v)}
  }

  def toMap:Map[A, B] = iterator.toMap

  def clearOldValues(threshTime:Long, f:(A, B) => Unit): Unit ={
    val it = getEntrySet.iterator()
    while(it.hasNext){
      val entry = it.next()
      if(entry.getValue.timestamp < threshTime){
        f(entry.getKey, entry.getValue.value)
        logDebug("removing key " + entry.getKey)
        it.remove()
      }
    }
  }

  def clearOldValues(threshTime:Long): Unit ={
    clearOldValues(threshTime, (_, _) => ())
  }

  private def currentTime: Long = System.currentTimeMillis

  def getTimeStampedValue(key:A):Option[TimeStampedValue[B]] = {
    Option(internalMap.get(key))
  }

  def getTimeStamp(key:A):Option[Long] = {
    getTimeStampedValue(key).map(_.timestamp)
  }

}
