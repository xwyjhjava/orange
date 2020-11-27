package com.dreams.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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

    val spark = SparkSession.builder()
      .appName("word count")
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val fileRDD: RDD[String] = sc.textFile("data/testdata.txt")
    val wordRDD: RDD[String] = fileRDD.flatMap(x => {x.split(" ")})
    val pairRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    //=======shuffle============
    val reduceRDD: RDD[(String, Int)] = pairRDD.reduceByKey(_ + _)

    val reverseRDD: RDD[(Int, Int)] = reduceRDD.map(x => (x._2, 1))
    reverseRDD.foreach(println)

    reverseRDD.distinct()

  }




}
