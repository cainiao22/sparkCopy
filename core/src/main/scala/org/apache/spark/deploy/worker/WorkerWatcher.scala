package org.apache.spark.deploy.worker

import akka.actor.{Address, AddressFromURIString, Actor}
import akka.remote.RemotingLifecycleEvent
import org.apache.spark.Logging
import org.apache.spark.deploy.SendHeartbeat

/**
 * Actor which connects to a worker process and terminates the JVM if the connection is severed.
 * Provides fate sharing between a worker and its associated child processes.
 */
private[spark] class WorkerWatcher(workerUrl:String) extends Actor with
Logging {

  override def preStart(): Unit ={
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    logInfo(s"connecting to worker $workerUrl")
    val worker = context.actorSelection(workerUrl)
    worker ! SendHeartbeat // need to send a message here to initiate connection
  }


  // Used to avoid shutting down JVM during tests
  private[delploy] var isShutdown = false
  private[deploy] def setTesting(testing:Boolean) = isTesting = testing
  private var isTesting = false

  // Lets us filter events only from the worker's actor system
  private val expectedHostPort = AddressFromURIString(workerUrl).hostPort
  private def isWorker(address:Address) = address.hostPort == expectedHostPort

  def exitNonZero = if(isTesting) isShutdown = true else System.exit(-1)

  override def receive = {

  }
}
