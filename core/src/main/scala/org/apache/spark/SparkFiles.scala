package org.apache.spark

import java.io.File

/**
 * Resolves paths to files added through `SparkContext.addFile()`.
 */
object SparkFiles {

  /**
   * Get the absolute path of a file added through `SparkContext.addFile()`.
   */
  def get(fileName:String):String = {
    new File(getRootDirectory()).getAbsolutePath
  }

  /**
   * Get the root directory that contains files added through `SparkContext.addFile()`.
   */
  def getRootDirectory():String = {
    SparkEnv.get.sparkFilesDir
  }
}
