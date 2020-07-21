package com.xiaoi.bank

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/16 15:48
 * @version V1.0
 * @description order 入mysql
 */
object SaveOrderIntoMysql {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("save data")
      .master("local[*]")
      .getOrCreate()


    val orderDF: DataFrame = sparkSession.read
      .option("header", true)
      .load("D:\\data\\cmb\\order")


    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val url = "jdbc:mysql://172.16.20.11:3306/recommend_baoxian?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true"
    val table = "mock_order_for_beibuwan"

    orderDF
      .write.mode(SaveMode.Overwrite)
      .jdbc(url, table, properties)

  }

}
