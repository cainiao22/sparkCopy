package org.apache.spark.ui

import javax.servlet.http.HttpServletRequest

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils
import org.eclipse.jetty.servlet.ServletContextHandler
import org.json4s.JsonAST.{JNothing, JValue}

import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

/**
 * The top level component of the UI hierarchy that contains the server.
 *
 * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
 * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
 */
private[spark] class WebUI(securityManager: SecurityManager,
                           port: Int,
                           conf: SparkConf,
                           basePath: String = "") {

  protected val tabs = ArrayBuffer[WebUITab]
  protected val handlers = ArrayBuffer[ServletContextHandler]
  protected var serverInfo: Option[ServerInfo] = None
  protected val localHostName = Utils.lo

}

/**
 * A tab that represents a collection of pages.
 * The prefix is appended to the parent address to form a full path, and must not contain slashes(斜杠).
 */
private[spark] class WebUITab(parent: WebUI, val prefix: String) {
  val pages = ArrayBuffer[WebUIPage]()
  val name = prefix.capitalize

  /** Attach a page to this tab. This prepends the page's prefix with the tab's own prefix. */
  def attachPage(page: WebUIPage): Unit = {
    //去除最后的那个"/"
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** Get a list of header tabs from the parent UI. */
  def headerTabs: Seq[WebUITab] = parent.getTabs
}

/**
 * A page that represents the leaf node in the UI hierarchy.
 *
 * The direct parent of a WebUIPage is not specified as it can be either a WebUI or a WebUITab.
 * If the parent is a WebUI, the prefix is appended to the parent's address to form a full path.
 * Else, if the parent is a WebUITab, the prefix is appended to the super prefix of the parent
 * to form a relative path. The prefix must not contain slashes.
 */
private[spark] abstract class WebUIPage(var prefix: String) {
  def render(request: HttpServletRequest): Seq[Node]

  def renderJson(request: HttpServletRequest): JValue = JNothing
}