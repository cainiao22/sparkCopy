package org.apache.spark.util

import java.io.{BufferedOutputStream, FileOutputStream, IOException, PrintWriter}
import java.net.URI
import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.io.CompressionCodec
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

  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat = new SimpleDateFormat("yyyy/MMdd HH:mm:ss")
  }

  private val fileSystem = Utils.getHadoopFileSystem(new URI(logDir))
  var fileIndex = 0

  // Only used if compression is enabled
  private lazy val compressionCodec = CompressionCodec.createCodec(sparkConf)

  // Only defined if the file system scheme is not local
  private var hadoopDataStream: Option[FSDataOutputStream] = None

  private var writer: Option[PrintWriter] = None

  /**
   * Start this logger by creating the logging directory.
   */
  def start(): Unit = {
    createLogDir()
  }

  /**
   * Create a logging directory with the given path.
   */
  private def createLogDir(): Unit = {
    val path = new Path(logDir)
    if (fileSystem.exists(path)) {
      if (overwrite) {
        logWarning("log dir %s already exists. overwriting ...".format(logDir))
        fileSystem.delete(path, true)
      } else {
        throw new IOException("log dir %s already exists".format(logDir))
      }
    }
    if (!fileSystem.mkdirs(path)) {
      throw new IOException("error in creating log dir: %s".format(logDir))
    }
    if (dirPermissions.isDefined) {
      val fsStatus = fileSystem.getFileStatus(path)
      if (fsStatus.getPermission.toShort != dirPermissions.get.toShort) {
        fileSystem.setPermission(path, dirPermissions.get)
      }
    }
  }

  /**
   * Create a new writer for the file identified by the given path.
   * If the permissions are not passed in, it will default to use the permissions
   * (dirPermissions) used when class was instantiated.
   */
  private def createWriter(fileName: String, perms: Option[FsPermission] = None): PrintWriter = {
    val logPath = logDir + "/" + fileName
    val uri = new URI(logPath)
    val path = new Path(logPath)
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefauleLocal = defaultFs == null || defaultFs == "file"

    val dstream =
      if((isDefauleLocal && uri.getScheme == null) || uri.getScheme == "file"){
        //second parameter is wether to append
        new FileOutputStream(uri.getPath, !overwrite)
      }else {
        hadoopDataStream = Some(fileSystem.create(path, overwrite))
        hadoopDataStream.get
      }

    perms.orElse(dirPermissions).foreach(p => fileSystem.setPermission(path, p))
    val bstream = new BufferedOutputStream(dstream, outputBufferSize)
    val cstream = if(compress) compressionCodec.compressedOutputStream(bstream) else bstream
    new PrintWriter(cstream)
  }


  /**
   * Start a writer for a new file, closing the existing one if it exists.
   * @param fileName Name of the new file, defaulting to the file index if not provided.
   * @param perms Permissions to put on the new file.
   */
  def newFile(fileName: String = "", perms: Option[FsPermission] = None): Unit = {
    fileIndex += 1
    writer.foreach(_.close())
    val name = fileName match {
      case "" => fileIndex.toString
      case _ => fileName
    }
    writer = Some(createWriter(name, perms))
  }
}
