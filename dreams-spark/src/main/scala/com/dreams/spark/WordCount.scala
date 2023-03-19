package com.dreams.spark

import org.apache.spark.{SparkContext, SparkJobInfo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.RDDInfo

import scala.util.Random

/**
 * @package com.dreams.spark
 * @author ming
 * @date 2020/1/7 10:28
 * @version V1.0
 * @description demo helloworld
 *              源码分析
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    val start: Long = System.nanoTime()
    val spark = SparkSession.builder()
      .appName("word count")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    // path的block数和minPartitions取最大值，作为分区数
    val fileRDD: RDD[String] = sc.textFile("", 10)


    val array = Array(1,2,3,4,5)
    val value: RDD[Int] = sc.parallelize(array)

    val count: Long = value.count()

    println(count)

    val interval: Long = System.nanoTime() - start
    println("interval == " + interval)

//    Thread.sleep(Long.MaxValue)


//    val sc: SparkContext = spark.sparkContext
//    val fileRDD: RDD[String] = sc.textFile("data/testdata.txt")
//    val wordRDD: RDD[String] = fileRDD.flatMap(x => {x.split(" ")})
//    val pairRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
//    //=======shuffle============
//    val reduceRDD: RDD[(String, Int)] = pairRDD.reduceByKey(_ + _)
//
//    val reverseRDD: RDD[(Int, Int)] = reduceRDD.map(x => (x._2, 1))
//    reverseRDD.foreach(println)
//    reverseRDD.distinct()

  }




}
