package org.apache.spark.util

import java.io.File
import java.net.URI

import org.apache.commons.lang3.SystemUtils
import org.apache.spark.Logging

/**
 * Created by Administrator on 2017/5/23.
 */
private[spark] object Utils extends Logging {

  /**
   * Whether the underlying operating system is Windows.
   */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
   * Pattern for matching a Windows drive, which contains only a single alphabet character.
   */
  val windowsDriver = "(a-zA-Z)".r

  /**
   * Get the ClassLoader which loaded Spark.
   */
  def getSparkClassLoader = getClass.getClassLoader

  def formatWindowsPath(path:String):String = path.replace("\\", "/")

  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  def resolveURI(path: String, testWindows: Boolean = false): URI = {

    // In Windows, the file separator is a backslash(反斜线), but this is inconsistent(不一致) with the URI format
    val windows = isWindows || testWindows
    val formattedPath = if(windows) formatWindowsPath(path) else path
    val uri = new URI(formattedPath)
    if(uri.getPath == null){
      throw new IllegalArgumentException(s"Given path is malformed: $uri")
    }
    uri.getScheme match {
      case windowsDriver(d) if windows =>
        new URI("file:/" + uri.toString.stripPrefix("/"))
      case null =>
        val fragment = uri.getFragment
        val part = new File(uri.getPath).toURI
        new URI(part.getScheme, part.getPath, fragment)
      case _ =>
        uri
    }
  }

  /** Resolve a comma-separated list of paths. */
  def resolveURIs(paths:String, testWindows:Boolean = false):String = {
    if(paths == null || paths.trim.isEmpty){
      ""
    }else{
      paths.split(",").map{p => Utils.resolveURI(p, testWindows)}.mkString(",")
    }
  }
}
