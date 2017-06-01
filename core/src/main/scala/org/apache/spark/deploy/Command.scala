package org.apache.spark.deploy

import scala.collection.mutable

/**
 * Created by Administrator on 2017/6/1.
 */
private[spark] case class Command(
     mainClass: String,
     arguments: Seq[String],
     environment: mutable.Map[String, String],
     classPathEntries: Seq[String],
     libraryPathEntries: Seq[String],
     extraJavaOptions: Option[String] = None) {

}
