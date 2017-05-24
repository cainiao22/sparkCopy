package org.apache.spark.deploy

/**
  * Created by Administrator on 2017/5/24.
  */
object SparkSubmit {

   private val YARN = 1
   private val STANDALONE = 2
   private val MESOS = 4
   private val LOCAL = 8
   private val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL

   private var clusterManager:Int = LOCAL

   private val SPARK_SHELL = "spark-shell"
   private val PYSPARK_SHELL = "pyspark-shell"

   def main(args:Array[String]): Unit ={
     val appArgs = new SparkSubmitArguments(args)
   }
 }
