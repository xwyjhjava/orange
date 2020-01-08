package com.dreams.spark

import java.util.logging.Level

import org.apache.spark.sql.SparkSession

/**
 * @package com.dreams.spark
 * @author ming
 * @date 2020/1/7 10:28
 * @version V1.0
 * @description demo helloworld
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("word count")
      .master("local[2]")
      .getOrCreate()

    spark.read.csv("")

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //prepare data
    val array = Array.apply(("A", 1), ("B", 2), ("A", 3))
    //make rdd
    val rdd = sc.makeRDD(array)
    //map and reduce
    rdd.map(x => {
      (x._1, x._2 + 1)
    }).collect().foreach(println)
    println("============================")
    rdd.flatMap(x => List((x._1, x._2 + 1)))
      .foreach(println)
    println("===========================")
  }

}
