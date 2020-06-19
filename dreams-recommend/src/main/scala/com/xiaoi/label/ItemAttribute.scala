package com.xiaoi.label

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * @Package com.xiaoi.label
 * @author ming
 * @date 2020/4/17 13:13
 * @version V1.0
 * @description item 的属性
 */
object ItemAttribute {

//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("com").setLevel(Level.OFF)


  private val ACTION_PATH = "D:\\ming\\bigdata\\datasupermarket\\action"

  def run(): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("item attribute")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext

    //    (uid, orderDate, userId, itemId, qty, amt)
    val actionDF: DataFrame = sparkSession.read
      .option("header", true)
      // TODO: 需要在Hbase中进行验证
      //      .option("inferSchema", true)
      .csv(ACTION_PATH)

    actionDF.printSchema()

    import sparkSession.implicits._

    val itemAttrRDD: RDD[(String, Int, String, Int, Int, String)] = actionDF.map(row => {
      val uid: String = row.getAs[String]("uid")
      val orderDate: String = row.getAs[String]("orderDate")
      val userId: String = row.getAs[String]("userId")
      val itemId: String = row.getAs[String]("itemId")
      val qty: String = row.getAs[String]("qty")
      val amt: String = row.getAs[String]("amt")
      (itemId, (uid, userId, orderDate, qty, amt))
    }).rdd
      .groupByKey()
      .mapPartitions(partition => {

        // (itemId, log_count, first_order_datetime, user_count, session_count, money)
        val rest = new ListBuffer[(String, Int, String, Int, Int, String)]

        while (partition.hasNext) {
          val iter: (String, Iterable[(String, String, String, String, String)]) = partition.next()
          val itemId: String = iter._1
          // (uid, userId, orderDate, qty, amt)
          val itemList: Iterable[(String, String, String, String, String)] = iter._2
          // 销售总数量
          val log_count: Int = itemList.size
          // 首单时间
          val first_order_datetime: String = itemList.map(_._3).toList.sortBy(x => x).take(1)(0)
          // 有多少用户买了该商品
          val user_count: Int = itemList.map(_._2).filter(!_.equals("")).toList.distinct.size
          // 订单数
          val session_count: Int = itemList.map(_._1).toList.distinct.size
          // 销售总金额
          val money: String = itemList.map(_._5).filter(!_.equals("")).map(_.toDouble).sum.formatted("%.2f")

          rest += ((itemId, log_count, first_order_datetime, user_count, session_count, money))
        }
        rest.iterator
      })


  }


  def main(args: Array[String]): Unit = {
    run()
  }



}
