package org.apache.spark.deploy.master

/**
 * Allows Master to persist any state that is necessary in order to recover from a failure.
 * The following semantics are required:
 *   - addApplication and addWorker are called before completing registration of a new app/worker.
 *   - removeApplication and removeWorker are called at any time.
 * Given these two requirements, we will have all apps and workers persisted, but
 * we might not have yet deleted apps or workers that finished (so their liveness must be verified
 * during recovery).
 */
private[spark] trait PersistenceEngine {

  def addApplication(app:ApplicationInfo)

  def removeApplication(app:ApplicationInfo)

  def addWorker(worker:WorkerInfo)

  def removeWorker(worker:WorkerInfo)

  def addDriver(driver:DriverInfo)

  def removeDriver(driver:DriverInfo)

  /**
   * Returns the persisted data sorted by their respective ids (which implies that they're
   * sorted by time of creation).
   */
  def readPersistedData():(Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo])

  def close(){}
}

private[spark] class BlackHolePersistenceEngine extends PersistenceEngine {
  override def addApplication(app: ApplicationInfo) {}
  override def removeApplication(app: ApplicationInfo) {}
  override def addWorker(worker: WorkerInfo) {}
  override def removeWorker(worker: WorkerInfo) {}
  override def addDriver(driver: DriverInfo) {}
  override def removeDriver(driver: DriverInfo) {}

  override def readPersistedData() = (Nil, Nil, Nil)
}
