package org.apache.spark.scheduler

/**
 * Created by QDHL on 2017/7/26.
 */
object TaskLocality extends Enumeration {
  val PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY = Value

  type TaskLocality = Value

  def isAllowed(constraint:TaskLocality, condition:TaskLocality):Boolean = {
    condition <= constraint
  }
}
