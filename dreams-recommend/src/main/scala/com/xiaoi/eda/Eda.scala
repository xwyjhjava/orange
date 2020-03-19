package com.xiaoi.eda

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

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
    val df: Dataset[Row] = spark.read
      .option("inferSchema", true)
      .textFile("D:\\data\\rec_min.txt")
      .map(x => {
        val array: Array[String] = x.split("\\|")
        (array(0), array(1), array(2), array(3), array(4), array(5),
          array(6), array(7), array(8), array(9), array(10),
          array(11), array(12), array(13))
      })
      // uid 表示一个交易回合（交易小票）， 也就是用户一次结账的order ID
      .toDF("uid", "sldate", "userId", "itemId", "itemName",
        "dptno", "dptname", "bandno", "bandname", "qty",
        "amt", "gender", "birthday", "createdate")


    df.select("itemName", "qty", "amt").show(50)
  }

}
