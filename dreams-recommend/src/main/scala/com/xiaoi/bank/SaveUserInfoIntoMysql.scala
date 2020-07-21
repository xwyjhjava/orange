package com.xiaoi.bank

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/16 15:32
 * @version V1.0
 * @description TODO
 */
object SaveUserInfoIntoMysql {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("save data")
      .master("local[*]")
      .getOrCreate()

    val sqlContext: SQLContext = sparkSession.sqlContext


    val userDF: DataFrame = sparkSession.read
      .option("header", true)
      .load("D:\\data\\cmb\\user")

    val count: Long = userDF.count()
    println(count)


    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val url = "jdbc:mysql://172.16.20.11:3306/recommend_baoxian?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true"
    val table = "mock_user_for_beibuwan"

    userDF
      .limit(10000)
      .write.mode(SaveMode.Overwrite)
      .jdbc(url, table, properties)

  }

}
