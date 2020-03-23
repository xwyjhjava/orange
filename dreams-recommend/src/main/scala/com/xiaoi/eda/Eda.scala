package com.xiaoi.eda

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @Package com.xiaoi.eda
 * @author ming
 * @date 2020/3/17 13:35
 * @version V1.0
 * @description data  eda
 */
class Eda {



}

object Eda{

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder()
      .appName("eda")
      .master("local[*]")
      .getOrCreate()


    // 准备数据
    import spark.implicits._
    val df: DataFrame = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("D:\\ming\\bigdata\\datasupermarket\\supermarket.csv")

    df.printSchema()

    df.show(10)

    println("===================")

    import org.apache.spark.sql.functions._
//    df.select("itemId", "sldate")


//    df.map(row => {
//      val itemId: Integer = row.getAs[Integer]("itemId")
////      val sldate: Timestamp = row.getAs[Timestamp]("sldate")
//      val sldate: Timestamp = row.getAs[Timestamp]("sldate")
//
//      val sldate_prefix: String = sldate.toString.split(" ")(0)
//      val len: Int = sldate.toString.length
//      (itemId, sldate_prefix, len)
//    }).toDF("itemId", "sldate", "len")
//      .filter($"len" =!= 21)
//      .show(5)


//    df.select("itemName", "qty", "amt").show(50)




    val calendar_df: DataFrame = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("D:\\ming\\bigdata\\datasupermarket\\calendar.csv")

    calendar_df.show()





  }

}
