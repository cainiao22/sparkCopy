package org.apache.spark.deploy.worker

import java.io.{IOException, FileOutputStream, InputStream, File}

import org.apache.spark.Logging
import org.apache.spark.deploy.Command
import org.apache.spark.util.Utils

/**
 * * Utilities for running commands with the spark classpath.
 */
private[spark]
object CommandUtils extends Logging {

  /**
   * 构建完整命令
   * java -cp $classPath -XX:MaxPermSize=128M -Djava.library.path=$joined -Xms${memory} $extraOpts -Xmx${memory} $mainClass $arguments
   * @param command
   * @param memory
   * @param sparkHome
   * @return
   */
  def buildCommandSeq(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    val runner = getEnv("JAVA_HOME", command).map(_ + "/bin/java").getOrElse("java")
    Seq(runner) ++ buildJavaOpts(command, memory, sparkHome) ++ Seq(command.mainClass) ++
      Seq(command.arguments)
  }

  private def getEnv(key: String, command: Command): Option[String] = {
    command.environment.get(key).orElse(Option(System.getenv(key)))
  }

  /**
   * Attention: this must always be aligned with the environment variables in the run scripts and
   * the way the JAVA_OPTS are assembled there.
   *
   * java -cp $classPath -XX:MaxPermSize=128M -Djava.library.path=$joined -Xms${memory} $extraOpts -Xmx${memory}
   */
  def buildJavaOpts(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    //-Xms 初始  -Xmx 最大
    val memoryOpts = Seq(s"-Xms${memory}", s"-Xmx${memory}")
    val extraOpts = command.extraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq())

    // Exists for backwards compatibility with older Spark versions
    val workLocalOpts = Option(System.getenv("SPARK_JAVA_OPTS"))
      .map(Utils.splitCommandString)
      .getOrElse(Nil)

    if(workLocalOpts.length > 0){
      logWarning("SPARK_JAVA_OPTS was set on the worker. It is deprecated in Spark 1.0.")
      logWarning("Set SPARK_LOCAL_DIRS for node-specific storage locations.")
    }

    val libraryOpts =
      if(command.libraryPathEntries.length > 0){
        val joined = command.libraryPathEntries.mkString(File.pathSeparator)
        Seq(s"-Djava.library.path=$joined")
      }else{
        Seq()
      }

    val permGenOpt = Seq("-XX:MaxPermSize=128M")

    // Figure out our classpath with the external compute-classpath script
    val ext = if(System.getProperty("os.name").startsWith("windows")) ".cmd" else ".sh"
    val classPath = Utils.excutaAndGetOutput(
      Seq(sparkHome + "/bin/compute-class" + ext),
      extraEnvironment = command.environment)

    val userClassPath = command.classPathEntries ++ Seq(classPath)
    //File.pathSeparator:  windows->';' linux->':'
    Seq("-cp", userClassPath.filterNot(_.isEmpty).mkString(File.pathSeparator)) ++
      permGenOpt ++ libraryOpts ++ extraOpts ++ memoryOpts

  }

  /** Spawn a thread that will redirect a given stream to a file */
  def redirectStream(in:InputStream, file:File): Unit ={
    val out = new FileOutputStream(file)

    new Thread("redirect output to " + file){
      override def run(): Unit ={
        try{
          Utils.coupStream(in, out, true)
        }catch {
          case e:IOException =>
            logInfo("Redirection to " + file + " closed: " + e.getMessage)
        }
      }
    }.start()
  }
}
