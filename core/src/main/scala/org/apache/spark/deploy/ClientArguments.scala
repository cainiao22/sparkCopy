package org.apache.spark.deploy

import org.apache.log4j.Level

import scala.collection.mutable.ListBuffer

/**
 * Command-line parser for the driver client.
 */
private[spark] class ClientArguments(args:Array[String]) {

  val defaultCores = 1
  val defaultMemory = 512

  var cmd: String = ""
  var logLevel = Level.INFO

  //launch
  var master: String = ""
  var jarUrl: String = ""
  var mainClass: String = ""
  var supervise: Boolean = false
  var memory: Int = defaultMemory
  var cores: Int = defaultCores
  private var _driverOptions = ListBuffer[String]()

  //原有list不支持修改，这个类型可以append
  def driverOptions = _driverOptions.toSeq

  //for kill
  var driverId: String = ""

  parse(args.toList)

  //Array没有 "::" 操作
  def parse(args: List[String]): Unit = args match {
    case ("--cores" | "-c") :: value :: tail =>
      cores = value.toInt
      parse(tail)
    case ("--memory" | "-m") :: value :: tail =>
      memory = value.toInt
      parse(tail)
    case ("--supervise" | "-s") :: tail =>
      supervise = true
      parse(tail)
    case ("--help") :: tail =>
      printUsageAndExit(0)
    case ("--verbose" | "-v") :: tail =>
      logLevel = Level.INFO
      parse(tail)
    case "launch" :: _master :: _jarUrl :: _mainClass :: tail =>
      cmd = "launch"
      if(!ClientArguments.isValidJarUrl(_jarUrl)){
        println(s"Jar url '${_jarUrl}' is not in valid format.")
        println(s"Must be a jar file path in URL format (e.g. hdfs://XX.jar, file://XX.jar)")
        printUsageAndExit(-1)
      }
      jarUrl = _jarUrl
      mainClass = _mainClass
      master = _master
      _driverOptions ++= tail
    case "kill"::_master::_driverId::tail =>
      cmd = "kill"
      master = _master
      driverId = _driverId
    case _ =>
      printUsageAndExit(1)

  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    // TODO: It wouldn't be too hard to allow users to submit their app and dependency jars
    //       separately similar to in the YARN client.
    val usage =
      s"""
         |Usage: DriverClient [options] launch <active-master> <jar-url> <main-class> [driver options]
         |Usage: DriverClient kill <active-master> <driver-id>
         |
      |Options:
         |   -c CORES, --cores CORES        Number of cores to request (default: $defaultCores)
         |   -m MEMORY, --memory MEMORY     Megabytes of memory to request (default: $defaultMemory)
         |   -s, --supervise                Whether to restart the driver on failure
         |   -v, --verbose                  Print more debugging output
     """.stripMargin
    System.err.println(usage)
    System.exit(exitCode)
  }
}

object ClientArguments {
  def isValidJarUrl(s:String):Boolean = s.matches("(.+):(.+)jar")
}
