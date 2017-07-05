package org.apache.spark

import java.io.File

/**
 * Created by Administrator on 2017/7/5.
 */
private[spark] class HttpFileServer(securityManager: SecurityManager) extends Logging {

  var baseDir:File = null
  var fileDir:File = null
  var httpServer:Httpserver
}
