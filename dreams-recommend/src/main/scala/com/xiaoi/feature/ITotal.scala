package com.xiaoi.feature

import org.apache.spark.sql.{Column, DataFrame, Dataset, RelationalGroupedDataset, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/**
 * @Package com.xiaoi.feature
 * @author ming
 * @date 2020/3/13 16:08
 * @version V1.0
 * @description item 特征
 */


//root
//|-- uid: integer (nullable = true)
//|-- sldate: timestamp (nullable = true)
//|-- userId: long (nullable = true)
//|-- itemId: integer (nullable = true)
//|-- itemName: string (nullable = true)
//|-- dptno: integer (nullable = true)
//|-- dptname: string (nullable = true)
//|-- bandno: integer (nullable = true)
//|-- bandname: string (nullable = true)
//|-- qty: double (nullable = true)
//|-- amt: double (nullable = true)
//|-- gender: integer (nullable = true)
//|-- birthday: timestamp (nullable = true)
//|-- createdate: timestamp (nullable = true)
class ITotal(private val spark: SparkSession) extends Serializable {


  /**
   *
   * @param df  data_clean数据
   * @return  (item, 交易回合数,  交易回合数ratio)
   * @description
   */
  private def get_i_total_orders(df: Dataset[Row]): Dataset[Row] ={

    //交易回合总数
    val total_uid: Long = df.select("uid")
      .distinct()
      .count()

    //item 交易回合数
    val dataFrame: DataFrame = df.select("itemId", "uid")
//      .distinct()
      .groupBy("itemId")
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
  private def get_i_total_items(df: Dataset[Row]): Dataset[Row]={

    val total_item: Long = df.select("itemId")
      .distinct()
      .count()

    val dataFrame: DataFrame = df.select("itemId", "pluname")
      .groupBy("itemId")
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
  private def get_i_total_distinct_users(df: Dataset[Row]): Dataset[Row] ={

    val base_df: DataFrame = df.select("uid", "itemId")
      .groupBy("uid")
      .agg(
        count("itemId").as("i_count")
      )
    //删选出每张订单只有一个商品的记录
      .filter($"i_count" === 1)

    //总独立商品购买次数
    val i_total_only_count: Long = base_df.count()

    val i_only_frame: Dataset[Row] = base_df.join(df.select("uid", "itemId"), "uid")
      .select("itemId", "i_count")
      .groupBy("itemId")
      .agg(
        //每个item被单独购买的次数
        sum("i_count").as("i_total_distinct_users"),
        //每个item独立购买次数 /  总独立商品购买次数
        (sum("i_count") / i_total_only_count).as("i_total_distinct_users_ratio")
      )
    i_only_frame.orderBy(desc("i_total_distinct_users"))
  }


  /**
   *
   * @param df  data clean 数据
   * @return
   * @description  平均每单购物数量
   */
  private def get_i_average_basket(df: DataFrame): DataFrame ={

    val itemRDD: RDD[(Integer, Integer)] = df.select("uid", "itemId")
      .map(row => {
        val sessionId: Integer = row.getAs[Integer]("uid")
        val itemId: Integer = row.getAs[Integer]("itemId")
        (itemId, sessionId)
      }).rdd


    // 计算每个回合的购物量
//     购物回合  , 每回合item数量
//    (sessionId, itemCountInEachCount)
    val session2itemCountMap: Map[Integer, Int] = itemRDD
        .groupBy(_._2)
        .mapValues(iter => {
          iter.size
        })
        .collect()
        .toMap

    // driver 将 map 广播出去
    val session2itemCountMapBroad: Broadcast[Map[Integer, Int]] = spark.sparkContext.broadcast(session2itemCountMap)


    val resultRDD: RDD[(Integer, String)] = itemRDD.groupByKey()
      .mapValues(iter => {
        val sessionList: List[Integer] = iter.toList
        // 计算每个item有多少回合
        val sessionSize: Int = sessionList.size
        //从广播变量中获取session的购买量Map
        val sessionMap: Map[Integer, Int] = session2itemCountMapBroad.value

        var totalItem2EverySession = 0
        for (session <- sessionList) {
          // 当前session中的item数量
          val itemCount: Int = sessionMap.get(session).get
          totalItem2EverySession = totalItem2EverySession + itemCount
        }
        // 计算含本商品的每单（回合）的平均购买数量
        val i_average_basket_ratio: Float = totalItem2EverySession / sessionSize
        i_average_basket_ratio.formatted("%.2f")
      })
    val resultDF: DataFrame = resultRDD.toDF("item_id", "i_average_basket")
    resultDF.filter($"i_average_basket" =!= "1.00").show(10)
    resultDF
  }



  /**
   *
   * @param df
   * @description 获取item价格相关信息
   * @return (item_id, 求和count, 最小值min, 最大值max, 平均数avg,  标准差stddev, 偏度skewness)
   */
  private def get_i_price_info(df: DataFrame): DataFrame ={

    val resultDF: DataFrame = df.select("itemId", "amt")
        .groupBy("itemId")
        .agg(
          count("amt").as("count"),
          min("amt").as("min"),
          max("amt").as("max"),
          avg("amt").as("avg"),
          stddev("amt").as("stddev"),
          skewness("amt").as("skewness")
        )
    resultDF
  }


  /**
   *
   * @param df  data_clean 数据
   * @return    （item_id）
   * @decription item的时间分布
   */
  private def get_i_time_distribute(df: DataFrame): DataFrame ={

    // 按照 年、月、日、时 提取时间
    val itemTimeDF: DataFrame = df
      .select("itemId", "sldate", "amt")
      .withColumn("year", year($"sldate"))
      .withColumn("month", month($"sldate"))
      .withColumn("day", dayofmonth($"sldate"))
      .withColumn("hour", hour($"sldate"))

    val yearDF: DataFrame = group_by_cols(itemTimeDF, "itemId", "year")
    val monthDF: DataFrame = group_by_cols(itemTimeDF, "itemId", "month")
    val dayDF: DataFrame = group_by_cols(itemTimeDF, "itemId", "day")
    val hourDF: DataFrame = group_by_cols(itemTimeDF, "itemId", "hour")
    
    df
  }


  /**
   *
   * @param df
   * @param col1
   * @param col2
   * @return
   * @description 根据规定的cols进行groupBy
   */
  private def group_by_cols(df: DataFrame, col1: String, col2: String): DataFrame ={

    val resultDF: DataFrame = df.groupBy(col1, col2)
      .agg(
        format_number(count("amt"), 0).as("count"),
        format_number(min("amt"), 2).as("min"),
        format_number(max("amt"), 2).as("max"),
        format_number(avg("amt"), 2).as("avg"),
        format_number(stddev("amt"), 2).as("stddev"),
        format_number(skewness("amt"), 2).as("skewness")
      )

    resultDF.printSchema()

    resultDF
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


    val df: DataFrame = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("D:\\ming\\bigdata\\datasupermarket\\supermarket.csv")


    //    df.show(5)
//
//    val count: Long = df.count()
//    println("total count =>" + count)
//
    val uid_total: Long = df.select("uid").distinct().count()
    println("uid total =>" + uid_total)



    import spark.implicits._

    val tmp: Long = df.select("itemId", "uid").filter($"itemId" === "30380003").count()
    println(tmp)


    val iTotal = new ITotal(spark)

//    val result: Dataset[Row] = iTotal.get_i_total_orders(df)
//
//    result.orderBy(desc("i_orders_count")).show(5)

    df.printSchema()


    val value: Dataset[Row] = iTotal.get_i_time_distribute(df)





  }

}
