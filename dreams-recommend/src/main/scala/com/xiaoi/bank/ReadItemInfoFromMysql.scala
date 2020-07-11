package com.xiaoi.bank

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/11 18:24
 * @version V1.0
 * @description TODO
 */
object ReadItemInfoFromMysql {


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("read item info")
      .master("local[*]")
      .getOrCreate()


    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")


    val url = "jdbc:mysql://172.16.20.23:3306/recommend_baoxian?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true"


    // 读i_basic_attr表
    val table = "i_basic_attr"
    val itemInfoDF: DataFrame = sparkSession.read.jdbc(url, table, properties)


    val selectedItemInfoDF: DataFrame = itemInfoDF.select("item_id", "category_id")

    selectedItemInfoDF.show()

    selectedItemInfoDF
      .coalesce(1)
      .write
      .option("header", true)
      .csv("D:\\data\\cmb\\itemCategoryCSV")









  }

}
