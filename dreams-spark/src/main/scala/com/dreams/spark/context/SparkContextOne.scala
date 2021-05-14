package com.dreams.spark.context

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

import java.util.concurrent.TimeUnit

/**
 * @Package com.dreams.spark.knowledge
 * @author ming
 * @date 2021/1/11 17:57
 * @version V1.0
 * @description TODO
 */
object SparkContextOne {


  def main(args: Array[String]): Unit = {

    val start: Long = System.nanoTime()

    val session: SparkSession = SparkSession.builder()
      .appName("spark context thread")
      .master("local")
      .getOrCreate()

    val sc: SparkContext = session.sparkContext
    val sqc: SQLContext = session.sqlContext

    val end: Long = System.nanoTime()
    val cost: Long = end - start
    println(s"cost time: ${TimeUnit.NANOSECONDS.toSeconds(cost)}s")

  }

}
