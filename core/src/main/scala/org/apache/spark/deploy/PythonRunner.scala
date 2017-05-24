package org.apache.spark.deploy

/**
 * A main class used by spark-submit to launch Python applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
object PythonRunner {

  def main (args: Array[String]){
    val pythonFile = args(0)
    val pyFiles = args(1)
    val otherArgs = args.slice(2, args.length)//返回指定的元素
    val pythonExec = sys.env.get("PYSPARK_PYTHON").getOrElse("python")
  }

  /**
   * Format the python file path so that it can be added to the PYTHONPATH correctly.
   *
   * Python does not understand URI schemes in paths. Before adding python files to the
   * PYTHONPATH, we need to extract the path from the URI. This is safe to do because we
   * currently only support local python files.
   */
  def formatPath(path:String, testWindows:Boolean = false):String = {

  }
}
