package org.apache.spark.util

/**
 * A class loader which makes findClass accesible to the child
 */
private[spark] class ParentClassLoader(parent:ClassLoader) extends ClassLoader(parent) {

  override def findClass(name:String) = {
    super.findClass(name)
  }

  override def loadClass(name:String):Class[_] = {
    super.loadClass(name)
  }

}
