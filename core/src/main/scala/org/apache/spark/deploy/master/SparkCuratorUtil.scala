package org.apache.spark.deploy.master

import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.{SparkConf, Logging}
import org.apache.zookeeper.KeeperException

/**
 * Created by Administrator on 2017/6/7.
 */
object SparkCuratorUtil extends Logging {

  val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  val ZK_SESSION_TIMEOUT_MILLIS = 60000
  val RETRY_WAIT_MILLIS = 5000
  val MAX_RECONNECT_ATTEMPTS = 3

  def newClient(conf: SparkConf):CuratorFramework = {
    val ZK_URL = conf.get("spark.deploy.zookeeper.url")
    val zk = CuratorFrameworkFactory.newClient(ZK_URL, ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
    new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))

    zk.start()
    zk
  }

  def mkdir(zk:CuratorFramework, path:String): Unit ={
    if(zk.checkExists().forPath(path) == null){
      try{
        zk.create().creatingParentsIfNeeded().forPath(path)
      }catch {
        case nodeExist:KeeperException.NodeExistsException =>
          //do nothing
        case e:Exception => throw e
      }
    }
  }
}
