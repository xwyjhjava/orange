package com.dreams.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Q4: 请输出每个分区下，每个province的去重登陆人数。输出结构为 pt，province，cnt_login
 */
object LoginDataETL_04 {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("loginData ETL 04")
      .master("local")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("Error")

    val loginCountDF: DataFrame = sparkSession.sql(
      """
        |select
        | substring(logtime,0,10) as pt, province, count(distinct(account_id)) as cnt_login from login_data_location
        |group by substring(logtime,0,10),province
        |order by cnt_login desc
        |""".stripMargin
    )

    loginCountDF.createTempView("loginCountDF")

    sparkSession.sql(
      """
        |insert overwrite table login_province_count select * from loginCountDF
        |""".stripMargin
    )

  }

}
