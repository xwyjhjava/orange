package com.xiaoi.feature

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/**
 * @Package com.xiaoi.feature
 * @author ming
 * @date 2020/3/13 16:08
 * @version V1.0
 * @description item 特征
 */
class ITotal(private val spark: SparkSession) extends Serializable {


  /**
   *
   * @param df  data_clean数据
   * @return  (item, 交易回合数,  交易回合数ratio)
   * @description
   */
  def get_i_total_orders(df: Dataset[Row]): Dataset[Row] ={

    //交易回合总数
    val total_uid: Long = df.select("uid")
      .distinct()
      .count()

    //item 交易回合数
    val dataFrame: DataFrame = df.select("prodno", "uid")
//      .distinct()
      .groupBy("prodno")
      .agg(
        count("uid").as("i_total_orders"),
        (count("uid").cast(DataTypes.FloatType) / total_uid).as("i_total_orders_ratio")
      )
    dataFrame.orderBy(desc("i_total_orders"))
  }


  /**
   *
   * @param df data clean 数据
   * @return  （item, 交易记录数， 交易记录数/总交易记录数）
   */
  def get_i_total_items(df: Dataset[Row]): Dataset[Row]={

    val total_item: Long = df.select("prodno")
      .distinct()
      .count()

    val dataFrame: DataFrame = df.select("prodno", "pluname")
      .groupBy("prodno")
      .agg(
        count("pluname").as("i_total_item"),
        (count("pluname") / total_item).as("i_total_items_ratio")
      )
    dataFrame.orderBy(desc("i_total_item"))
  }



  import spark.implicits._
  /**
   *
   * @param df  data clean 数据
   * @return    （itemId,  商品独立卖出次数， 商品独立卖出次数占比）
   * @description  每个交易回合只有一个商品
   */
  def get_i_total_distinct_users(df: Dataset[Row]): Dataset[Row] ={

    val base_df: DataFrame = df.select("uid", "prodno")
      .groupBy("uid")
      .agg(
        count("prodno").as("i_count")
      )
    //删选出每张订单只有一个商品的记录
      .filter($"i_count" === 1)

    //总独立商品购买次数
    val i_total_only_count: Long = base_df.count()

    val i_only_frame: Dataset[Row] = base_df.join(df.select("uid", "prodno"), "uid")
      .select("prodno", "i_count")
      .groupBy("prodno")
      .agg(
        //每个item被单独购买的次数
        sum("i_count").as("i_total_distinct_users"),
        //每个item独立购买次数 /  总独立商品购买次数
        (sum("i_count") / i_total_only_count).as("i_total_distinct_users_ratio")
      )
    i_only_frame.orderBy(desc("i_total_distinct_users"))
  }


  // TODO: 含有本商品的交易回合的交易数量的平均量
  /**
   *
   * @param df  data clean 数据
   * @return
   * @description  平均每单购物数量
   */
  def get_i_average_basket(df: DataFrame): DataFrame ={

    df.select("uid", "prodno")



  }





}

object ITotal {

  def main(args: Array[String]): Unit = {


    val logger = LoggerFactory.getLogger(getClass)
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
      .toDF("uid", "sldate", "vipno", "prodno", "pluname",
        "dptno", "dptname", "bandno", "bandname", "qty",
        "amt", "gender", "birthday", "createdate")


//    df.show(5)
//
//    val count: Long = df.count()
//    println("total count =>" + count)
//
    val uid_total: Long = df.select("uid").distinct().count()
    println("uid total =>" + uid_total)


    val tmp: Long = df.select("prodno", "uid").filter($"prodno" === "30380003").count()
    println(tmp)


    val iTotal = new ITotal(spark)

//    val result: Dataset[Row] = iTotal.get_i_total_orders(df)
//
//    result.orderBy(desc("i_orders_count")).show(5)

    val value: Dataset[Row] = iTotal.get_i_total_distinct_users(df)

    value.show(5)

  }

}
