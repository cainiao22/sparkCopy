package org.apache.spark.deploy

import akka.actor.Actor
import akka.actor.Actor.Receive
import org.apache.spark.Logging

/**
 * Proxy that relays messages to the driver.
 */
private class ClientActor extends Actor with Logging {



  override def preStart(): Unit ={

  }

  override def receive: Receive = {

  }
}

class Client {

}
