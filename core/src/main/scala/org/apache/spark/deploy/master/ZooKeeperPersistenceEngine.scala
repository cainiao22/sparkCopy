package org.apache.spark.deploy.master

import akka.serialization.Serialization
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.{Logging, SparkConf}
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConversions._

/**
 * Created by Administrator on 2017/6/7.
 */
class ZooKeeperPersistenceEngine(serialization: Serialization, conf: SparkConf)
extends PersistenceEngine with Logging {

  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/master_status"
  val zk:CuratorFramework = SparkCuratorUtil.newClient(conf)

  SparkCuratorUtil.mkdir(zk, WORKING_DIR)

  override def addApplication(app: ApplicationInfo): Unit = {
    serializeIntoFile(WORKING_DIR + "/app_" + app.id, app)
  }

  override def removeApplication(app: ApplicationInfo): Unit = {
    zk.delete().forPath(WORKING_DIR + "/app_" + app.id)
  }

  /**
   * Returns the persisted data sorted by their respective ids (which implies that they're
   * sorted by time of creation).
   */
  override def readPersistedData(): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    val sortedFiles = zk.getChildren.forPath(WORKING_DIR).toList.toSeq
    val appFiles = sortedFiles.filter(_.startsWith("app_"))
    val apps = appFiles.map(deserializeFromFile[ApplicationInfo]).flatten
    val driverFiles = sortedFiles.filter(_.startsWith("driver_"))
    val drivers = driverFiles.map(deserializeFromFile[DriverInfo]).flatten
    val workerFiles = sortedFiles.filter(_.startsWith("worker_"))
    val workers = workerFiles.map(deserializeFromFile[WorkerInfo]).flatten

    (apps, drivers, workers)
  }

  override def addWorker(worker: WorkerInfo): Unit = {
    serializeIntoFile(WORKING_DIR + "/worker_" + worker.id, worker)
  }

  override def addDriver(driver: DriverInfo): Unit = {
    serializeIntoFile(WORKING_DIR + "/driver_" + driver.id, driver)
  }

  override def removeWorker(worker: WorkerInfo): Unit = {
    zk.delete().forPath(WORKING_DIR + "/worker_" + worker.id)
  }

  override def removeDriver(driver: DriverInfo): Unit = {
    zk.delete().forPath(WORKING_DIR + "/driver_" + driver.id)
  }

  override def close(): Unit ={
    zk.close()
  }

  private def serializeIntoFile(path:String, value:AnyRef): Unit ={
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, serialized)
  }

  def deserializeFromFile[T](filename:String)(implicit m:Manifest[T]):Option[T] = {
    val fileData = zk.getData.forPath(WORKING_DIR + "/" + filename)
    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    val serializer = serialization.serializerFor(clazz)
    try{
      Some(serializer.fromBinary(fileData).asInstanceOf[T])
    }catch {
      case e:Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(WORKING_DIR + "/" + filename)
        None
    }
  }
}
