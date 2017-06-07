package org.apache.spark.util

import java.io.File
import java.net.{InetAddress, Inet4Address, NetworkInterface, URI, URL, URLConnection}

import org.apache.commons.lang3.SystemUtils
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.util.Try

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

  def getContextOrSparkClassLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  def classIsLoadable(clazz: String): Boolean = {
    Try {
      Class.forName(clazz, false, getContextOrSparkClassLoader)
    }.isSuccess
  }

  /**
   * Format a Windows path such that it can be safely passed to a URI.
   */
  def formatWindowsPath(path: String): String = path.replace("\\", "/")

  /**
   * Indicates whether Spark is currently running unit tests.
   */
  def isTesting = {
    sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
  }

  /**
   * Strip the directory from a path name
   */
  def stripDirectory(path: String): String = {
    new File(path).getName
  }

  /**
   * Return a well-formed URI for the file described by a user input string.
   *
   * If the supplied path does not contain a scheme, or is a relative path, it will be
   * converted into an absolute path with a file:// scheme.
   */
  def resolveURI(path: String, testWindows: Boolean = false): URI = {

    // In Windows, the file separator is a backslash(反斜线), but this is inconsistent(不一致) with the URI format
    val windows = isWindows || testWindows
    val formattedPath = if (windows) formatWindowsPath(path) else path
    val uri = new URI(formattedPath)
    if (uri.getPath == null) {
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
  def resolveURIs(paths: String, testWindows: Boolean = false): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").map { p => Utils.resolveURI(p, testWindows) }.mkString(",")
    }
  }

  /** Return all non-local paths from a comma-separated list of paths. */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = isWindows || testWindows
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val formattedPath = if (windows) formatWindowsPath(p) else p
        new URI(formattedPath).getScheme match {
          case windowsDriver(d) if windows => false
          case "local" | "file" | null => false
          case _ => true
        }
      }
    }
  }

  def checkHost(host: String, message: String = "") {
    assert(host.indexOf(':') == -1, message)
  }

  lazy val localIpAddress: String = findLocalIpAddress()
  lazy val localIpAddressHostname: String = getAddressHostName(localIpAddress)

  private def findLocalIpAddress(): String = {
    val defaultIpOverrde = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverrde != null) {
      defaultIpOverrde
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        for (ni <- NetworkInterface.getNetworkInterfaces) {
          for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress
            && !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
              " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return addr.getHostAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address.getHostAddress
    }
  }

  private var customHostname: Option[String] = None

  def setCustomHostname(hostname: String): Unit = {
    customHostname = Some(hostname)
  }

  def getAddressHostName(address: String): String = {
    InetAddress.getByName(address).getHostName
  }

  def localHostName(): String = {
    customHostname.getOrElse(localIpAddressHostname)
  }

  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef) = {
    obj.getClass.getSimpleName.replace("$", "")
  }
}
