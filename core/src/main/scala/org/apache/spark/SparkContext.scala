package org.apache.spark

/**
 * Created by Administrator on 2017/5/25.
 */
class SparkContext {

}

object SparkContext extends Logging {

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to SparkContext.
   *
   * instance: jar:file:/D:/localRepository/org/apache/commons/commons-lang3/3.3.2/commons-lang3-3.3.2.jar!/org/apache/commons/lang3/StringUtils.class
   */
  def jarOfClass(cls:Class[_]):Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if(uri != null){
      val uriStr = uri.toString
      if(uriStr.startsWith("jar:file:")){
        return Some(uriStr.substring("jar:file:".length, uriStr.indexOf("!")))
      }
    }
    None
  }

  /**
   * Find the JAR that contains the class of a particular object, to make it easy for users
   * to pass their JARs to SparkContext. In most cases you can call jarOfObject(this) in
   * your driver program.
   */
  def jarOfObject(obj:AnyRef):Option[String] = jarOfClass(obj.getClass)
}
