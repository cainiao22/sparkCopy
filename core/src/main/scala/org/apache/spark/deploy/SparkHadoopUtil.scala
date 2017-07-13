package org.apache.spark.deploy


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkException, Logging}

/**
 * Created by Administrator on 2017/7/3.
 */
class SparkHadoopUtil extends Logging {
  val conf:Configuration = newConfiguration()
  UserGroupInformation.setConfiguration(conf)


  def newConfiguration():Configuration = new Configuration()

  def isYarnMode(): Boolean = { false }

  def getSecretKeyFromUserCredentials(key: String): Array[Byte] = { null }

  def addSecretKeyToUserCredentials(key: String, secret: String) {}

}

object SparkHadoopUtil {

  private val hadoop = {
    val yarnMode = java.lang.Boolean.valueOf(
      System.getProperty("SPARK_YARN_MODE", System.getenv("SPARK_YARN_MODE"))    )
      if(yarnMode){
        try{
          Class.forName("org.apache.spark.deploy.yarn.YarnSparkHadoopUtil")
            .newInstance()
            .asInstanceOf[SparkHadoopUtil]
        }catch {
          case e:Exception => throw new SparkException("Unable to load YARN support", e)
        }
      } else {
        new SparkHadoopUtil
      }
  }

  def get:SparkHadoopUtil = {
    hadoop
  }
}