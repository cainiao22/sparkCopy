package org.apache.spark

import akka.actor.ActorSystem

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, Akka actor system, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a thread-local variable, so each thread that accesses these
 * objects needs to have the right SparkEnv set. You can get the current environment with
 * SparkEnv.get (e.g. after creating a SparkContext) and set it with SparkEnv.set.
 *
 * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
 * in a future release.
 */
class SparkEnv(
                val httpFileServer: HttpFileServer,
                val securityManager: SecurityManager,
                val sparkFilesDir: String) {

}

object SparkEnv extends Logging {
  private val env = new ThreadLocal[SparkEnv]
  @volatile private var lastSetSparkEnv: SparkEnv = _

  def set(e: SparkEnv): Unit = {
    lastSetSparkEnv = e
    env.set(e)
  }

  /**
   * Returns the ThreadLocal SparkEnv, if non-null. Else returns the SparkEnv
   * previously set in any thread.
   */
  def get: SparkEnv = {
    Option(env.get()).getOrElse(lastSetSparkEnv)
  }
}
