package org.apache.spark.deploy.client

import akka.remote.RemotingLifecycleEvent
import org.apache.spark.deploy.master.Master
import org.apache.spark.util.Utils

import scala.concurrent.duration._

import akka.actor._
import org.apache.spark.{SparkException, Logging, SparkConf}
import org.apache.spark.deploy._

/**
 * Interface allowing applications to speak with a Spark deploy cluster. Takes a master URL,
 * an app description, and a listener for cluster events, and calls back the listener when various
 * events occur.
 *
 * @param masterUrls Each url should look like spark://host:port.
 */
private[spark] class AppClient(
                                actorSystem: ActorSystem,
                                masterUrls: Array[String],
                                applicationDescription: ApplicationDescription,
                                listener: AppClientListener,
                                conf: SparkConf) extends Logging {

  val REGISTRATION_TIMEOUT = 20.seconds
  val REGISTRATION_RETRIES = 3

  var masterAddress:Address = null
  var actor:ActorRef = null
  var appId:String = null
  var registered = false
  var activeMasterUrl:String = null

  class ClientActor extends Actor with Logging {
    var master:ActorSelection = null
    var alreadyDisconnected = false // To avoid calling listener.disconnected() multiple times
    var alreadyDead = false // To avoid calling listener.dead() multiple times
    var registrationRetryTimer: Option[Cancellable] = None //todo ??这是啥玩意

    override def preStart(): Unit ={
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
      try{
        registerWithMaster()
      }catch {
        case e:Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          context.system.stop(self)
      }
    }

    def tryRegisterAllMasters(): Unit ={
      for(masterUrl <- masterUrls){
        logInfo("Connecting to master " + masterUrl + "...")
        val actor = context.actorSelection(Master.toAkkaUrl(masterUrl))
        actor ! RegisterApplication(applicationDescription)
      }
    }

    def registerWithMaster(): Unit ={
      tryRegisterAllMasters()

      var retries = 0
      registrationRetryTimer = Some(
        context.system.scheduler.schedule(REGISTRATION_TIMEOUT, REGISTRATION_TIMEOUT){
        Utils.tryOrExit{
          retries += 1
          if(registered){
            registrationRetryTimer.foreach(_.cancel())
          }else if(retries >= REGISTRATION_RETRIES){
            markDead("All masters are unresponsive! Giving up.")
          }else{
            tryRegisterAllMasters()
          }
        }
      })
    }

    def changeMaster(url:String): Unit ={
      activeMasterUrl = url
      master = context.actorSelection(Master.toAkkaUrl(url))
      masterAddress = activeMasterUrl match {
        case Master.sparkUrlRegex(host, port) =>
          Address("akka.tcp", Master.systemName, host, port.toInt)
        case x =>
          throw new SparkException("Invalid spark URL: " + x)
      }
    }

    override def receive() = {

      case ApplicationRemoved(message) =>
        markDead(message)
        context.stop(self)

      case MasterChanged(masterUrl, masterWebUrl) =>
        logInfo("Master has changed, new master is at " + masterUrl)
        changeMaster(masterUrl)
        alreadyDisconnected = false
        sender ! MasterChangeAcknowledged(appId)
    }

    /**
     * Notify the listener that we disconnected, if we hadn't already done so before.
     */
    def markDisconnected() {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    def markDead(reason:String): Unit ={
      if(!alreadyDead){
        listener.dead(reason)
        alreadyDead = true
      }
    }
  }


}
