package org.apache.spark

import java.io.File

import com.google.common.io.Files
import org.apache.spark.util.Utils

/**
 * Created by Administrator on 2017/7/5.
 */
private[spark] class HttpFileServer(securityManager: SecurityManager) extends Logging {

  var baseDir: File = null
  var fileDir: File = null
  var jarDir: File = null
  var httpServer: HttpServer = null
  var serverUri: String = null

  def initialize(): Unit = {
    baseDir = Utils.createTempDir()
    fileDir = new File(baseDir, "files")
    jarDir = new File(baseDir, "jars")
    fileDir.mkdir()
    jarDir.mkdir()
    logInfo("HTTP File server directory is " + baseDir)
    httpServer = new HttpServer(baseDir, securityManager)
    httpServer.start()
    serverUri = httpServer.uri
  }

  def addJar(file:File):String = {
    addFileToDir(file, jarDir)
    serverUri + "/jars/" + file.getName
  }

  def addFileToDir(file: File, dir:File): Unit ={
    // Check whether the file is a directory. If it is, throw a more meaningful exception.
    // If we don't catch this, Guava throws a very confusing error message:
    //   java.io.FileNotFoundException: [file] (No such file or directory)
    // even though the directory ([file]) exists.
    if (file.isDirectory) {
      throw new IllegalArgumentException(s"$file cannot be a directory.")
    }
    Files.copy(file, new File(dir, file.getName))
    dir + "/" + file.getName
  }
}
