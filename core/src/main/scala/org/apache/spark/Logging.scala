package org.apache.spark

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.slf4j.impl.StaticLoggerBinder
import org.slf4j.{Logger, LoggerFactory}

/**
 * Created by Administrator on 2017/5/23.
 */
trait Logging {

  private var log_ : Logger = null;

  protected def log: Logger = {
    if(log_ == null){
      initializeIfNecessary()
      var className = this.getClass.getName
      if(className.endsWith("$")){
        className = className.substring(0, className.length -1)
      }
      log_ = LoggerFactory.getLogger(className)
    }
    log_
  }

  // Log methods that take only a String
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  private def initializeIfNecessary(): Unit ={
    if(!Logging.initialized){
      Logging.initLock.synchronized{
        if(!Logging.initialized){
          initializeLogging()
        }
      }
    }
  }

  private def initializeLogging(): Unit ={
    val binder = StaticLoggerBinder.getSingleton
    val usingLog4j = binder.getLoggerFactoryClassStr.endsWith("Log4jLoggerFactory")
    val log4jInitialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
    if(!log4jInitialized && usingLog4j){
      val defaultLogProps = "org/apache/spark/log4j-defaults.properties"
      Option(Utils.getSparkClassLoader.getResource(defaultLogProps)) match {
        case Some(url) =>
          PropertyConfigurator.configure(url)
          log.info(s"uing sperk's default log4j profile:$defaultLogProps")
        case None =>
          System.err.println(s"Spark was unable to load $defaultLogProps")
      }
    }
    Logging.initialized = true
    log
  }

}

private object Logging {

  @volatile
  private var initialized = false;

  val initLock = new Object

  try{
    val bridgeClass = Class.forName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if(!installed){
      bridgeClass.getMethod("install").invoke(null)
    }
  }catch {
    case e: ClassNotFoundException =>
  }

}
