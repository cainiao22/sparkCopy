package org.apache.spark.deploy


import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkContext, SparkException, Logging}

/**
 * Created by Administrator on 2017/7/3.
 */
class SparkHadoopUtil extends Logging {
  val conf:Configuration = newConfiguration()
  UserGroupInformation.setConfiguration(conf)

  /**
   * Runs the given function with a Hadoop UserGroupInformation as a thread local variable
   * (distributed to child threads), used for authenticating HDFS and YARN calls.
   *
   * IMPORTANT NOTE: If this function is going to be called repeated in the same process
   * you need to look https://issues.apache.org/jira/browse/HDFS-3545 and possibly
   * do a FileSystem.closeAllForUGI in order to avoid leaking Filesystems
   */
  def runAsSparkUser(func:() => Unit): Unit ={
    val user = Option(System.getenv("SPARK_USER")).getOrElse(SparkContext.SPARK_UNKNOWN_USER)
    if(user != SparkContext.SPARK_UNKNOWN_USER){
      logDebug("running as user: " + user)
      val ugi = UserGroupInformation.createRemoteUser(user)
      transferCredentials(UserGroupInformation.getCurrentUser, ugi)
      ugi.doAs(new PrivilegedExceptionAction[Unit] {
        def run: Unit = func()
      })
    }else {
      logDebug("running as SPARK_UNKNOWN_USER")
      func()
    }
  }

  def transferCredentials(source: UserGroupInformation, dest: UserGroupInformation): Unit ={
    for(token <- source.getTokens){
      dest.addToken(token)
    }
  }


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