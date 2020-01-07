package com.dreams.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Package com.dreams.spark
 * @author ming
 * @date 2020/1/7 18:32
 * @version V1.0
 * @description sparkContext test
 */
object SparkContextTest {
  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf()
      .setAppName("sparkContext test")
      .setMaster("local[2]")
    val sc = new SparkContext(sparkconf)

  }
}
