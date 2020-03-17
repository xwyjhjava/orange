package com.xiaoi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

/**
 * @Package com.xiaoi
 * @author ming
 * @date 2020/3/17 10:41
 * @version V1.0
 * @description 测试
 */
object MainTest {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder()
      .appName("item feature")
      .master("local[*]")
      .getOrCreate()

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
      .toDF("uid", "sldate", "userId", "itemId", "pluname",
        "dptno", "dptname", "bandno", "bandname", "qty",
        "amt", "gender", "birthday", "createdate")


    val uid_count: Long = df.select("uid").count()
    println("uid_count => " +  uid_count)

    val uid_distinct_count: Long = df.select("uid").distinct().count()
    println("uid_distinct_count =>" + uid_distinct_count)

    val uid_and_userId_count: Long = df.select("uid", "userId").count()
    println("uid_and_userId_count =>" + uid_and_userId_count)

    val uid_and_userId_distinct_count: Long = df.select("uid", "userId").distinct().count()
    println("uid_and_userId_distinct_count => " + uid_and_userId_distinct_count)


    import org.apache.spark.sql.functions._
    val item_df: Dataset[Row] = df.select("uid", "userId", "itemId")
      .groupBy("itemId")
      .agg(
        count("userId").as("userId_count")
      )
      .filter($"userId_count" === 1)

    val itemId_count: Long = item_df.select("itemId").count()
    println("itemId_count => " + itemId_count)

    val itemId_distinct_count: Long = item_df.select("itemId").count()
    println("itemId_distinct_count => " + itemId_distinct_count)


    val user_count: Long = df.select("userId").distinct().count()
    println("user_count =>" + user_count)


  }


}
