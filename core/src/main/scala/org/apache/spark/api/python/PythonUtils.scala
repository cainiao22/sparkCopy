package org.apache.spark.api.python

import java.io.{IOException, OutputStream, InputStream, File}

import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2017/5/25.
 */
private[spark] object PythonUtils {
  /** Get the PYTHONPATH for PySpark, either from SPARK_HOME, if it is set, or from our JAR */
  def sparkPythonPath: String = {
    val pythonPath = new ArrayBuffer[String]()
    for (sparkHome <- sys.env.get("SPARK_HOME")) {
      pythonPath += Seq(sparkHome, "python").mkString(File.separator)
      pythonPath += Seq(sparkHome, "python", "lib", "py4j-0.8.1-src.zip").mkString(File.separator)
    }
    pythonPath ++= SparkContext.jarOfObject(this)
    //pathSeparator 多个不同路径下的分割符，比如 a.txt, b.txt  此时pathSeparator代表','
    pythonPath.mkString(File.pathSeparator)
  }

  /** Merge PYTHONPATHS with the appropriate separator. Ignores blank strings. */
  def mergePythonPaths(paths: String*): String = {
    paths.filter(_ != "").mkString(File.pathSeparator)
  }
}

private[spark] class RedirectThread(
                                     in: InputStream,
                                     out: OutputStream,
                                     name: String
                                     ) extends Thread(name) {
  //确保其他主线程退出后，它也跟着退出
  setDaemon(true)

  override def run(): Unit = {
    scala.util.control.Exception.ignoring(classOf[IOException]){
      val buf = new Array[Byte](1024)
      var len = in.read(buf)
      while(len != -1){
        out.write(buf, 0, len)
        out.flush()
        len = in.read(buf)
      }
    }
  }
}
