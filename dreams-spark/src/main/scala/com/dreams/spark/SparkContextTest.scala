package com.dreams.spark

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

//    rdd.saveAsTextFile("hdfs://192.168.199.132:8020//test//rec_bak.txt")
//    rdd.saveAsTextFile("D:\\xiaoi\\patition_test")
//    rdd.foreach(println)

  }
}
