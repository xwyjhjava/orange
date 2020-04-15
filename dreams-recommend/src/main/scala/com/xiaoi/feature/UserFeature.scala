package com.xiaoi.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @Package com.xiaoi.feature
 * @author ming
 * @date 2020/4/15 14:27
 * @version V1.0
 * @description 用户特征中心
 */
object UserFeature {


  private val USER_INFO_PATH = "D:\\ming\\bigdata\\datasupermarket\\userInfo"
  private val ITEM_INFO_PATH = "D:\\ming\\bigdata\\datasupermarket\\itemInfo"
  private val ACTION_PATH = "D:\\ming\\bigdata\\datasupermarket\\action"


  //准备userInfo 数据
  private def readUserInfo(sparkSession: SparkSession) ={

    val userInfo: DataFrame = sparkSession.read
      .option("header", true)
      .csv(USER_INFO_PATH)

    import sparkSession.implicits._

    val userInfoDF: Dataset[(String, String, String)] = userInfo.map(row => {
      val userId: String = row.getAs[String]("userId")
      val birthday: String = row.getAs[String]("birthday")
      val createDate: String = row.getAs[String]("createDate")
      (userId, birthday, createDate)
    })
    userInfoDF
  }

  // 准备action 数据
  def readActionInfo(sparkSession: SparkSession) ={

    val actionInfo: DataFrame = sparkSession.read
      .option("header", true)
      .csv(ACTION_PATH)

    import sparkSession.implicits._

    val actionInfoDF: Dataset[(String, String, String, String, String, String, String)] = actionInfo.map(row => {
      val uid: String = row.getAs[String]("uid")
      val orderDate: String = row.getAs[String]("orderDate")
      val userId: String = row.getAs[String]("userId")
      val itemId: String = row.getAs[String]("itemId")
      val qty: String = row.getAs[String]("qty")
      val amt: String = row.getAs[String]("amt")
      (uid, userId, orderDate, userId, itemId, qty, amt)
    })
    actionInfoDF
  }



  // 基础特征















  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)


    val spark: SparkSession = SparkSession.builder()
      .appName("user feature")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext









  }

}
