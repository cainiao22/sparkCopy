package org.apache.spark.deploy.worker

import org.apache.spark.Logging
import org.apache.spark.deploy.Command

/**
 ** Utilities for running commands with the spark classpath.
 */
private[spark]
object CommandUtils extends Logging {

  def buildCommandSeq(command:Command,  memory:Int, sparkHome:String):Seq[String] = {
    val runner = getEnv("JAVA_HOME", command).map(_ + "/bin/java").getOrElse("java")
    //Seq(runner) ++
  }

  private def getEnv(key:String, command:Command):Option[String] = {
    command.environment.get(key).orElse(Option(System.getenv(key)))
  }

  /**
   * Attention: this must always be aligned with the environment variables in the run scripts and
   * the way the JAVA_OPTS are assembled there.
   */
  def buildJavaOpts(command: Command, memory:Int, sparkHome:String):Seq[String] = {
    //-Xms 初始  -Xmx 最大
    val memoryOpts = Seq(s"-Xms${memory}", s"-Xmx${memory}")
    val extraOpts = command.extraJavaOptions.map()

  }
}
