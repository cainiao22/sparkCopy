package org.apache.spark.serializer

import java.io.EOFException
import java.nio.ByteBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.util.NextIterator

import scala.reflect.ClassTag

/**
 * Created by QDHL on 2017/7/26.
 */
trait Serializer {
  def newInstance(): SerializerInstance
}

object Serializer {
  def getSerializer(serializer: Serializer): Serializer = {
    if (serializer == null) SparkEnv.get.serializer else serializer
  }
}

trait SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deSerialize[T: ClassTag](bytes: ByteBuffer): T

  def deSerialize[T: ClassTag](bytes: ByteBuffer, classLoader: ClassLoader): T
}

trait SerializationStream {
  def writeObject[T: ClassTag](t: T): SerializationStream

  def flush(): Unit

  def close(): Unit

  def writeAll[T: ClassTag](it: Iterator[T]): SerializationStream = {
    while (it.hasNext) {
      writeObject(it.next())
    }
    this
  }
}

trait DeserializationStream {
  def readObject[T: ClassTag](): T

  def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
   */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext(): Unit = {
      try{
        readObject[Any]()
      }catch {
        case eof:EOFException =>
          finished = true
      }
    }

    override protected def close(): Any = {
      DeserializationStream.this.close()
    }
  }
}
