package org.apache.spark.ui

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}

import org.apache.spark.{Logging, SecurityManager}

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.ContextHandlerCollection
import org.eclipse.jetty.servlet.{ServletHolder, ServletContextHandler}

/**
 * Created by Administrator on 2017/6/5.
 */
private[spark] object JettyUtils extends Logging {

  type Responder[T] = HttpServletRequest => T

  class ServletParams[T <% AnyRef](val responder: Responder[T],
                                    val contentType:String,
                                    val extractFn:T => String = (in:Any) => in.toString){}

  def createServlet[T <% AnyRef](
                                servletParam:ServletParams,
                                securityManager: SecurityManager
                                  ):HttpServlet = {
    new HttpServlet {
      //todo doGet实现
      override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit ={

      }
    }
  }


  /** Create a context handler that responds to a request with the given path prefix */
  def createServletHandler(path:String,
                            servlet:HttpServlet,
                            basePath:String = ""):ServletContextHandler = {

    val prefixPath = attachPrefix(basePath, path)
    val contextHandler = new ServletContextHandler()
    val holder = new ServletHolder(servlet)
    contextHandler.setContextPath(prefixPath)
    contextHandler.addServlet(holder, "/")
    contextHandler
  }

  private def attachPrefix(basePath:String, relativePath:String):String = {
    if(basePath == "") relativePath else (basePath + relativePath).stripSuffix("/")
  }

}

private[spark] case class ServerInfo(server:Server,
                                      boundPort:Int,
                                      rootHandler:ContextHandlerCollection)