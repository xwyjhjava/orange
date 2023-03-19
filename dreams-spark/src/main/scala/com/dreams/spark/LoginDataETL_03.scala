package com.dreams.spark

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

/**
 * 通过Q1，Q2加载的数据，将用户登陆表中的ip转化为对应的国家地区并落表（避免笛卡尔积）
 */
object LoginDataETL_03 {

  def main(args: Array[String]): Unit = {
    // ip_china.csv 为配置表，大小为6M，小于广播表默认10M的大小
    // 将ip_china.csv广播出去，用map join替代reduce join

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("loginData ETL 03")
      .master("local")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("Error")

    val sc: SparkContext = sparkSession.sparkContext
    val sql: SQLContext = sparkSession.sqlContext

    import sql.implicits._

//    sparkSession.sql("use default")
    val ipDF: DataFrame = sparkSession.sql(
      s"""
         | select
         |   ip_start,
         |   ip_end,
         |   long_ip_start,
         |   long_ip_end,
         |   country,
         |   province
         | from IP_CHINA
      """.stripMargin
    )

    val loginDF: DataFrame = sparkSession.sql(
      """
        | select
        |   account_id,
        |   ip,
        |   logtime
        |  from login_data
        |""".stripMargin
    )

    val broadcastDF: Broadcast[Array[Row]] = sc.broadcast(ipDF.collect())

    val ip2countryArray: List[(Long, Long, String, String)] = broadcastDF.value.toList.map(row => {
      (row.getAs[String]("long_ip_start").toLong,
        row.getAs[String]("long_ip_end").toLong, row.getAs[String]("country"),
        row.getAs[String]("province"))
    })

    val convertLoginDF: DataFrame = loginDF.map(row => {
      val account_id = row.getAs[String]("account_id")
      val ip: String = row.getAs[String]("ip")
      val logtime = row.getAs[String]("logtime")

      val countryAndProvinceTuple2: (String, String) = searchForRange(ip2countryArray, ip2Long(ip))
      val country = countryAndProvinceTuple2._1
      val province = countryAndProvinceTuple2._2

      (account_id, country, province, logtime)
    }).toDF("account_id", "country", "province", "logtime")

    // 落库落表
    convertLoginDF.createTempView("convertLoginDF")
    sparkSession.sql(
      """
        |insert overwrite table login_data_location select * from convertLoginDF
        |""".stripMargin
    )

  }



  // 范围查询，二分查找
  def searchForRange(source : List[(Long, Long, String, String)], target: Long): (String, String) ={
    var start = 0
    var end = source.length - 1
    var flag = true
    // 不在大范围内
    if(source(start)._1 > target || source(end)._2 < target){
      return ("unknown", "unknown")
    }
    while(start < end && flag){
      var middle = (start + end) >> 1
      if(source(middle)._1 == target || source(middle)._2 == target){
        start = middle
        flag = false
      } else if(source(middle)._1 < target){ // 在区间中
        if(target < source(middle)._2){
          start = middle
          flag = false
        }else{
          start = middle + 1
        }
      }else if(source(middle)._1 > target){
        end = middle - 1
      }
    }
    (source(start)._3, source(start)._4)
  }

  // ip2Long
  def ip2Long(ip: String): Long ={
    val ele: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for(i <- 0 until ele.length){
      ipNum = ele(i).toLong | ipNum << 8L
    }
    ipNum
  }

}


