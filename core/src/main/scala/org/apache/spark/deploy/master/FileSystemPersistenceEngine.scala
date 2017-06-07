package org.apache.spark.deploy.master

import java.io.{FileInputStream, DataInputStream, FileOutputStream, File}

import akka.serialization.Serialization
import org.apache.spark.Logging

/**
 * Created by Administrator on 2017/6/7.
 */
private[spark] class FileSystemPersistenceEngine(
                                                  val dir:String,
                                                  val serialization: Serialization)
extends PersistenceEngine with Logging {

  new File(dir).mkdir()

  override def addApplication(app: ApplicationInfo): Unit = {
    serializeIntoFile(new File(dir + File.separator + "app_" + app.id ), app)
  }

  override def removeApplication(app: ApplicationInfo): Unit = {
    new File(dir + File.separator + "app_" + app.id).delete()
  }

  /**
   * Returns the persisted data sorted by their respective ids (which implies that they're
   * sorted by time of creation).
   */
  override def readPersistedData(): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    val files = new File(dir).listFiles().toList.sortBy(_.getName)
    val appFiles = files.filter(_.getName.startsWith("app_"))
    val apps = appFiles.map(deserializeFromFile[ApplicationInfo])
    val driverFiles = files.filter(_.getName.startsWith("driver_"))
    val drivers = driverFiles.map(deserializeFromFile[DriverInfo])
    val workerFiles = files.filter(_.getName.startsWith("worker_"))
    val workers = workerFiles.map(deserializeFromFile[WorkerInfo])

    (apps, drivers, workers)
  }

  override def addWorker(worker: WorkerInfo): Unit = {
    serializeIntoFile(new File(dir + File.separator + "worker_" + worker.id ), worker)
  }

  override def addDriver(driver: DriverInfo): Unit = {
    serializeIntoFile(new File(dir + File.separator + "driver_" + driver.id ), driver)
  }

  override def removeWorker(worker: WorkerInfo): Unit = {
    new File(dir + File.separator + "worker_" + worker.id).delete()
  }

  override def removeDriver(driver: DriverInfo): Unit = {
    new File(dir + File.separator + "worker_" + driver.id).delete()
  }

  private def serializeIntoFile(file:File, value:AnyRef): Unit ={
    val created = file.createNewFile()
    if(!created) throw new IllegalStateException("could not create file: " + file)
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)

    val out = new FileOutputStream(file)
    out.write(serialized)
    out.close()
  }

  def deserializeFromFile[T](file:File)(implicit m:Manifest[T]):T = {
    val fileData = new Array[Byte](file.length().asInstanceOf[Int])
    val dis = new DataInputStream(new FileInputStream(file))
    dis.readFully(fileData)
    dis.close()

    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    val serializer = serialization.findSerializerFor(clazz)
    serializer.fromBinary(fileData).asInstanceOf[T]
  }
}
