package org.apache.spark.deploy.master

import akka.actor.ActorRef
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.spark.deploy.master.MasterMessages.{RevokedLeadership, ElectedLeader}
import org.apache.spark.{Logging, SparkConf}

/**
 * Created by Administrator on 2017/6/7.
 */
private[spark] class ZooKeeperLeaderElectionAgent(val masterActor: ActorRef,
                                                  masterUrl: String,
                                                  conf: SparkConf)
  extends LeaderElectionAgent with LeaderLatchListener with Logging {

  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/leader_election"

  private var zk: CuratorFramework = _
  private var leaderLatch: LeaderLatch = _
  private var status = LeadershipStatus.NOT_LEADER

  override def preStart(): Unit ={
    zk = SparkCuratorUtil.newClient(conf)
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    leaderLatch.addListener(this)

    leaderLatch.start()
  }

  override def preRestart(reason: scala.Throwable, message: scala.Option[scala.Any]) {
    logError("LeaderElectionAgent failed...", reason)
    super.preRestart(reason, message)
  }

  override def postStop(): Unit ={
    leaderLatch.close()
    zk.close()
  }



  override def isLeader: Unit = {
    synchronized{
      if(!leaderLatch.hasLeadership){
        return
      }else{
        logInfo("We have gained leadership")
        updateLeadershipStatus(true)
      }
    }
  }

  override def notLeader(): Unit = {
    synchronized{
      if(leaderLatch.hasLeadership){
        return
      }
      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }

  def updateLeadershipStatus(isLeader:Boolean): Unit ={
    if(isLeader && status == LeadershipStatus.NOT_LEADER){
      status = LeadershipStatus.LEADER
       masterActor ! ElectedLeader
    }else if(!isLeader && status == LeadershipStatus.LEADER){
      status = LeadershipStatus.NOT_LEADER
      masterActor ! RevokedLeadership
    }
  }

  override def receive = {
    case _ =>
  }
}

private object LeadershipStatus extends Enumeration {
  type LeadershipStatus = Value
  val LEADER, NOT_LEADER = Value
}
