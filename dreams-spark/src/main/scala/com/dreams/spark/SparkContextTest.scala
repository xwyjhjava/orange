package com.dreams.spark

import java.net.URI

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
/**
 * @Package com.dreams.spark
 * @author ming
 * @date 2020/1/7 18:32
 * @version V1.0
 * @description sparkContext test
 */
object SparkContextTest {

//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("apache").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("sparkContext test")
      .setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
//    sc.setLogLevel("OFF")
    val rdd: RDD[String] = sc.textFile("D:\\xiaoi\\rec_min.txt", 1)
//    val rdd: RDD[String] = sc.textFile("D:\\ideaworkspace\\BigDataArchitect\\bigdata-hadoop\\data\\hello.txt")
//    val rdd: RDD[String] = sc.textFile("hdfs://192.168.199.132:8020//test//rec.txt")
    println("partition number:", rdd.getNumPartitions)



  val baseRDD = rdd.map(x => x.split("\\|"))
    .map(x => (x(0), x(1)))

  val size: Long = baseRDD.count()
  println(size)

  val rdd2: RDD[(String, Int)] = baseRDD.map(x => (x._1, 1)).reduceByKey(_ + _)

  rdd2.take(5).foreach(println)
  baseRDD.take(5).foreach(println)


//    rdd.saveAsTextFile("hdfs://192.168.199.132:8020//test//rec_bak.txt")
//    rdd.saveAsTextFile("D:\\xiaoi\\patition_test")
//    rdd.foreach(println)


   Thread.sleep(Long.MaxValue)

  }



  def removeFile(path: String): Unit ={
    val uri = new URI(path)
    val schema: String = uri.getScheme();
    if(schema == null || schema.isEmpty){ //use local file

    }
    if(schema.equals("hdfs")){   //use hdfs file
    }




  }
}
