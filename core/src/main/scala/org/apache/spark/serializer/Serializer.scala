package org.apache.spark.serializer

import org.apache.spark.SparkEnv

/**
 * Created by QDHL on 2017/7/26.
 */
trait Serializer {
  def newInstance():SerializerInstance
}

object Serializer {
  def getSerializer(serializer: Serializer):Serializer = {
    if(serializer == null) SparkEnv.get.serializer else serializer
  }
}

trait SerializerInstance {

}
