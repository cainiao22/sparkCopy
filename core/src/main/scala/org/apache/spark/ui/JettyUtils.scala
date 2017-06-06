package org.apache.spark.ui

import org.apache.spark.Logging
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.ContextHandlerCollection

/**
 * Created by Administrator on 2017/6/5.
 */
private[spark] object JettyUtils extends Logging {



}

private[spark] case class ServerInfo(server:Server,
                                      boundPort:Int,
                                      rootHandler:ContextHandlerCollection)