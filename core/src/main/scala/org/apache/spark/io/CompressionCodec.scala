package org.apache.spark.io

import java.io.{InputStream, OutputStream}

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

/**
 * Created by Administrator on 2017/7/4.
 */
trait CompressionCodec {
  def compressedOutputStream(s:OutputStream):OutputStream

  def compressedInputStream(s:InputStream):InputStream
}

private[spark] object CompressionCodec {
  def createCodec(conf:SparkConf):CompressionCodec = {
    createCodec(conf, conf.get("spark.io.compression.codec", DEFAULT_COMPRESSION_CODEC))
  }

  def createCodec(conf:SparkConf, codecName:String):CompressionCodec = {
    val ctor = Class.forName(codecName, true, Utils.getContextOrSparkClassLoader)
      .getConstructor(classOf[SparkConf])
    ctor.newInstance(conf).asInstanceOf[CompressionCodec]
  }

  val DEFAULT_COMPRESSION_CODEC = classOf[LZFCompressionCodec].getName
}

/**
 * :: DeveloperApi ::
 * LZF implementation of [[org.apache.spark.io.CompressionCodec]].
 *
 * Note: The wire protocol for this codec is not guaranteed to be compatible across versions
 *       of Spark. This is intended for use as an internal compression utility within a single Spark
 *       application.
 */
class LZFCompressionCodec(conf: SparkConf) extends CompressionCodec {
  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new LZFOutputStream(s).setFinishBlockOnFlush(true)
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    new LZFInputStream(s)
  }
}

class SnappyCompressionCodec(conf:SparkConf) extends CompressionCodec {
  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.getInt("spark.io.compression.snappy.block.size", 32768)
    new SnappyOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    new SnappyInputStream(s)
  }
}
