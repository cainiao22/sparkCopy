package org.apache.spark

import java.io.File

import org.apache.spark.util.Utils
import org.eclipse.jetty.security.authentication.DigestAuthenticator
import org.eclipse.jetty.security.{HashLoginService, ConstraintMapping, ConstraintSecurityHandler}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.bio.SocketConnector
import org.eclipse.jetty.server.handler.{DefaultHandler, HandlerList, ResourceHandler}
import org.eclipse.jetty.util.security.{Password, Constraint}
import org.eclipse.jetty.util.thread.QueuedThreadPool


/**
 * Exception type thrown by HttpServer when it is in the wrong state for an operation.
 */
private[spark] class ServerStateException(message: String) extends Exception(message)

/**
 * An HTTP server for static content used to allow worker nodes to access JARs added to SparkContext
 * as well as classes created by the interpreter when the user types in code. This is just a wrapper
 * around a Jetty server.
 */
private[spark] class HttpServer(resourceBase: File, securityManager: SecurityManager)
  extends Logging {

  private var server: Server = null
  private var port: Int = -1

  def start(): Unit ={
    if(server != null){
      throw new ServerStateException("Server is already started")
    }else {
      logInfo("Starting HTTP Server")
      server = new Server()
      val connector = new SocketConnector()
      connector.setMaxIdleTime(60*1000)

      /**
       * 关闭Socket时会等待_soLingerTime时间，此时如果有数据还未发送完，则会发送这些数据；
       * 如果关闭了该选项，则Socket的关闭会立即返回，此时也有可能继续发送未发送完成的数据
       */
      connector.setSoLingerTime(-1)
      connector.setPort(0)
      server.addConnector(connector)

      val threadPool = new QueuedThreadPool()
      threadPool.setDaemon(true)
      server.setThreadPool(threadPool)

      val resHandler = new ResourceHandler
      val handlerList = new HandlerList
      handlerList.setHandlers(Array(resHandler, new DefaultHandler))

      if(securityManager.isAuthenticationEnabled()){
        logDebug("HttpServer is using security")
        val sh = setupSecurityHandler(securityManager)
        sh.setHandler(handlerList)
        server.setHandler(sh)
      }else{
        server.setHandler(handlerList)
      }
      server.start()
      port = server.getConnectors()(0).getLocalPort
    }
  }

  /**
   * Setup Jetty to the HashLoginService using a single user with our
   * shared secret. Configure it to use DIGEST-MD5 authentication so that the password
   * isn't passed in plaintext.
   */
  private def setupSecurityHandler(securityManager: SecurityManager):ConstraintSecurityHandler = {
    val constraint = new Constraint()
    // use DIGEST-MD5 as the authentication mechanism
    constraint.setName(Constraint.__DIGEST_AUTH)
    constraint.setRoles(Array("user"))
    constraint.setAuthenticate(true)
    constraint.setDataConstraint(Constraint.DC_NONE)

    val cm = new ConstraintMapping
    cm.setConstraint(constraint)
    cm.setPathSpec("/*")
    val sh = new ConstraintSecurityHandler
    // the hashLoginService lets us do a single user and
    // secret right now. This could be changed to use the
    // JAASLoginService for other options.
    val hashLogin = new HashLoginService()

    val userCred = new Password(securityManager.getSecretKey())
    if (userCred == null) {
      throw new Exception("Error: secret key is null with authentication on")
    }
    hashLogin.putUser(securityManager.getHttpUser(), userCred, Array("user"))
    sh.setLoginService(hashLogin)
    sh.setAuthenticator(new DigestAuthenticator());
    sh.setConstraintMappings(Array(cm))
    sh
  }

  def stop: Unit ={
    if(server == null){
      throw new ServerStateException("Server is already stopped")
    }
    server.stop()
    server = null
    port = -1
  }

  /**
   * Get the URI of this HTTP server (http://host:port)
   */
  def uri: String = {
    if (server == null) {
      throw new ServerStateException("Server is not started")
    } else {
      return "http://" + Utils.localIpAddress + ":" + port
    }
  }
}
