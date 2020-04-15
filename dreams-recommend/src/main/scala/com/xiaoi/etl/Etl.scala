package com.xiaoi.etl

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * @Package com.xiaoi.eda
 * @author ming
 * @date 2020/3/17 13:35
 * @version V1.0
 * @description data  etl , 原则上一份数据只需运行一次
 */
class Etl {



}

object Etl{

  private val ORIGINAL_DATA_PATH = "D:\\ming\\bigdata\\datasupermarket\\supermarket_sample.txt"
  private val USER_INFO_PATH = "D:\\ming\\bigdata\\datasupermarket\\userInfo"
  private val ITEM_INFO_PATH = "D:\\ming\\bigdata\\datasupermarket\\itemInfo"
  private val ACTION_PATH = "D:\\ming\\bigdata\\datasupermarket\\action"


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder()
      .appName("etl")
      .master("local[*]")
      .getOrCreate()


    // 准备数据
    /**
     * root
     * |-- uid: integer (nullable = true)
     * |-- sldate: timestamp (nullable = true)
     * |-- userId: long (nullable = true)
     * |-- itemId: integer (nullable = true)
     * |-- itemName: string (nullable = true)
     * |-- dptno: integer (nullable = true)
     * |-- dptname: string (nullable = true)
     * |-- bandno: integer (nullable = true)
     * |-- bandname: string (nullable = true)
     * |-- qty: double (nullable = true)
     * |-- amt: double (nullable = true)
     * |-- gender: integer (nullable = true)
     * |-- birthday: timestamp (nullable = true)
     * |-- createdate: timestamp (nullable = true)
     *
     *
     */

    val sc: SparkContext = spark.sparkContext

    val baseRDD: RDD[(String, String, String, String, String, String,
      String, String, String, String, String, String, String, String)] =

      sc.textFile(ORIGINAL_DATA_PATH)
      .map(x => {
        val array: Array[String] = x.split("\\|")
        // uid
        val uid: String = array(0)
        val orderDate: String = array(1)
        val userId: String = array(2)
        val itemId: String = array(3)
        val itemName: String = array(4)
        val dptNo: String = array(5)
        val dptName: String = array(6)
        val bandNo: String = array(7)
        val bandName: String = array(8)
        // 交易数量
        val qty: String = array(9)
        // 交易净额
        val amt: String = array(10)
        val gender: String = array(11)
        val birthday: String = array(12)
        val createDate: String = array(13)
        (uid, orderDate, userId, itemId, itemName, dptNo, dptName, bandNo, bandName, qty, amt, gender, birthday, createDate)
      })

    baseRDD.cache()

    // 准备userInfo数据
    val userInfoRDD: RDD[(String, String, String, String)] = baseRDD.map(ele => {
      val userId: String = ele._3
      val gender: String = ele._12
      // 会员生日
      val birthday: String = ele._13
      // 会员入会时间
      val creteDate: String = ele._14
      (userId, gender, birthday, creteDate)
    })

    // 准备itemInfo数据
    val itemInfoRDD: RDD[(String, String, String, String, String, String)] =
      baseRDD.map(ele => {
      val itemId: String = ele._4
      val itemName: String = ele._5
      val dptNo: String = ele._6
      val dptName: String = ele._7
      val bandNo: String = ele._8
      val bandName: String = ele._9
      (itemId, itemName, dptNo, dptName, bandNo, bandName)
    })

    // 准备action 数据
    val actionRDD: RDD[(String, String, String, String, String, String)] =
      baseRDD.map(ele => {
      // 交易号码（交易小票， 一个交易回合，一次结账的orderId）
      val uid: String = ele._1
      // 交易时间
      val orderDate: String = ele._2
      val userId: String = ele._3
      val itemId: String = ele._4
      val qty: String = ele._10
      val amt: String = ele._11
      (uid, orderDate, userId, itemId, qty, amt)
    })

    import spark.implicits._
    // save userInfo
    spark.createDataset(userInfoRDD)
      .toDF("userId", "gender", "birthday", "createDate")
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(USER_INFO_PATH)

    // save itemInfo
    spark.createDataset(itemInfoRDD)
      .toDF("itemId", "itemName",
        "dptNo", "dptName",
        "bandNo", "bandName")
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(ITEM_INFO_PATH)

    // save action
    spark.createDataset(actionRDD)
      .toDF("uid", "orderDate", "userId", "itemId",
      "qty", "amt")
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(ACTION_PATH)

  }

}
