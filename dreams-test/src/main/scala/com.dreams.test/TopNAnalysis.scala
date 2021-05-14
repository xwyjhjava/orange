package com.dreams.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 处理数据
 */
object TopNAnalysis {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("apache").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {
    val filePath = "D:\\data\\top.csv";
    val savePath = "D:\\data\\top"
    val yearStr = "2016"

    run(filePath, savePath, yearStr)


  }

  /**
   *
   * @param filePath 原始数据文件所在的位置
   * @param savePath 数据保存路径
   * @param yearStr 当前分析的年份
   */
  def run(filePath: String, savePath: String, yearStr: String): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("read log")
      .master("local")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

//    val rdd: RDD[String] = sc.textFile("D:\\data\\top.csv")
    val rdd: RDD[String] = sc.textFile(filePath)

    import sparkSession.implicits._
    // 分析 state 和 city的关系
    val stateDF: DataFrame = rdd.map(_.split(",")).filter(arr => arr(7) == yearStr)
      .map(arr => (arr(5), arr(4))).toDF("state", "city")

    import org.apache.spark.sql.functions._
    stateDF.groupBy("state").agg(collect_set("city").as("arrayCity"))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .json("file:///" + savePath + "\\" + yearStr + "_city")

    // 分析 city 和 restaurant 的关系
    val cityDF: DataFrame = rdd.map(_.split(",")).filter(arr => arr(7) == yearStr)
      .map(arr => (arr(4), arr(1))).toDF("city", "rest")

    cityDF.groupBy("city").agg(collect_set("rest").as("arrayRest"))
          .repartition(1)
          .write
          .mode(SaveMode.Overwrite)
          .json("file:///" + savePath + "\\" + yearStr + "_rest")





  }


}
