package com.xiaoi.feature

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.{LoggerFactory}
import org.apache.log4j.{Level, Logger}

/**
 * @Package com.xiaoi.feature
 * @author ming
 * @date 2020/3/13 16:08
 * @version V1.0
 * @description item 特征
 */
class ITotal {




}

object ITotal{

  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("item feature")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("inferSchema", true)
      .textFile("D:\\data\\rec_min.txt")
      .map(x => {
        val array: Array[String] = x.split("\\|")
        (array(0), array(1), array(2), array(3), array(4), array(5),
        array(6), array(7), array(8), array(9), array(10),
        array(11), array(12), array(13))
      })
      // uid 表示一个交易回合（交易小票）， 也就是用户一次结账的order ID
      .toDF("uid", "sldate", "vipno", "prodno", "pluname",
        "dptno", "dptname", "bandno", "bandname", "qty",
        "amt", "gender", "birthday", "createdate")

    df.show(5)

    val count: Long = df.count()
    println("total count =>" + count)

    val uid_total: Long = df.select("uid").distinct().count()
    println("uid total =>" + uid_total)




  }

}
