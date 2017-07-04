package org.apache.spark.util

import java.net.URI
import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * A generic class for logging information to file.
 *
 * @param logDir Path to the directory in which files are logged
 * @param outputBufferSize The buffer size to use when writing to an output stream in bytes
 * @param compress Whether to compress output
 * @param overwrite Whether to overwrite existing files
 */
private[spark] class FileLogger(
                                 logDir: String,
                                 sparkConf: SparkConf,
                                 hadoopConf: Configuration = SparkHadoopUtil.get.newConfiguration(),
                                 outputBufferSize: Int = 8 * 1024,
                                 compress: Boolean = false,
                                 overwrite: Boolean = false,
                                 dirPermissions: Option[FsPermission] = None
                                 ) extends Logging {

  private val dateFormat = new ThreadLocal[SimpleDateFormat](){
    override def initialValue():SimpleDateFormat = new SimpleDateFormat("yyyy/MMdd HH:mm:ss")
  }

  private val fileSystem = Utils.getHadoopFileSystem(new URI(logDir))
  var fileIndex = 0

  private lazy val compressionCodec = CompressionCodec.c


}
