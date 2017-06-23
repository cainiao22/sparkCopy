package org.apache.spark.scheduler

import java.util.concurrent.{Semaphore, LinkedBlockingQueue, LinkedBlockingDeque}

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until start() is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when it receives a SparkListenerShutdown event, which is posted using stop().
 */
private[spark] class LiveListenerBus extends SparkListenerBus with Logging {

  /* Cap the capacity of the SparkListenerEvent queue so we get an explicit error (rather than
  * an OOM exception) if it's perpetually being added to more quickly than it's being drained. */
  private val EVENT_QUEUE_CAPACITY = 10000
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](EVENT_QUEUE_CAPACITY)
  private var queueFullErrorMessageLogged = false
  private var started = false

  // A counter that represents the number of events produced and consumed in the queue
  private val eventLock = new Semaphore(0)

  private val listenerThread = new Thread("SparkListenerBus"){
    //所有用户线程全部退出的时候他才会退出
    setDaemon(true)
    override def run():Unit = Utils.logUncaughtExceptions{
      setDaemon(true)
      while(true){
        eventLock.acquire()
        //eventLock保证进来的线程数量不会超过n.但是之后的线程还是有可能出现并发
        LiveListenerBus.this.synchronized {
          val event = eventQueue.poll
          if(event == SparkListenerShutdown) {
            //Get out of the while loop and shutdown the daemon thread
            return
          }
          Option(event).foreach(postToAll)
        }
      }
    }
  }

  /**
   * Start sending events to attached listeners.
   *
   * This first sends out all buffered events posted before this listener bus has started, then
   * listens for any additional events asynchronously while the listener bus is still running.
   * This should only be called once.
   */
  def start(): Unit ={
    if(started){
      throw new IllegalStateException("listener bus already started!")
    }
    listenerThread.start()
    started = true
  }

  def post(event: SparkListenerEvent): Unit ={
    val eventAdded = eventQueue.offer(event)
    if(eventAdded){
      eventLock.release()
    }else{
      logQueueFullErrorMessage()
    }
  }

  /**
   * Log an error message to indicate that the event queue is full. Do this only once.
   */
  private def logQueueFullErrorMessage(): Unit = {
    if (!queueFullErrorMessageLogged) {
      if (listenerThread.isAlive) {
        logError("Dropping SparkListenerEvent because no remaining room in event queue. " +
          "This likely means one of the SparkListeners is too slow and cannot keep up with" +
          "the rate at which tasks are being started by the scheduler.")
      } else {
        logError("SparkListenerBus thread is dead! This means SparkListenerEvents have not" +
          "been (and will no longer be) propagated to listeners for some time.")
      }
      queueFullErrorMessageLogged = true
    }
  }


}
