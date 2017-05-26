package org.apache.spark.executor

import java.net.{URL, URLClassLoader}

import org.apache.spark.util.ParentClassLoader

/**
 * The addURL method in URLClassLoader is protected. We subclass it to make this accessible.
 * We also make changes so user classes can come before the default classes.
 */

private[spark] trait MutableURLClassLoader extends ClassLoader {
  def addURL(url: URL)

  def getURLs: Array[URL]
}

private[spark] class ChildExecutorURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends MutableURLClassLoader {

  private object userClassLoader extends URLClassLoader(urls, null) {
    override def addURL(url: URL): Unit = {
      super.addURL(url)
    }

    override def findClass(name: String): Class[_] = {
      super.findClass(name)
    }
  }

  private val parentClassLoader = new ParentClassLoader(parent)

  override def findClass(name: String): Class[_] = {
    try {
      userClassLoader.findClass(name)
    } catch {
      case e: ClassNotFoundException =>
        parentClassLoader.loadClass(name)
    }
  }

  override def addURL(url: URL): Unit = {
    userClassLoader.addURL(url)
  }

  override def getURLs() = {
    userClassLoader.getURLs()
  }

}


private[spark] class ExecutorURLClassLoader(urls:Array[URL], parent:ClassLoader)
  extends URLClassLoader(urls, parent) with MutableURLClassLoader {

  override def addURL(url:URL): Unit ={
    super.addURL(url)
  }
}
