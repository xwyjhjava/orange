package com.dreams.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Q5: 请输出总量数据下，存在登陆数据的各个province中，登陆时间最早的前3人及对应的登陆时间，若不满3人，需要留空。
 * 输出结构为 province，account_id_1, login_time_1, account_id_2, login_time_2, account_id_3, login_time_3
 */
object LoginDataETL_05 {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("loginData ETL 03")
      .master("local")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("Error")


    // 分组去重排序取topN
    val loginRankDF: DataFrame = sparkSession.sql(
      """
        |select t1.rank,t1.account_id,t1.province,t1.minLogtime as logtime
        |from (select row_number() over(partition by t.province order by t.minLogtime asc) as rank,
        |t.account_id,t.province,t.minLogtime
        |from (select account_id,province,MIN(logtime) as minLogtime
        |from login_data_location group by account_id,province having province != 'unknown') t) t1 where t1.rank < 4
        |""".stripMargin
    )

    import org.apache.spark.sql.functions._
    import sparkSession.sqlContext.implicits._
    // 列转行
    val accountPivotDF: DataFrame = loginRankDF.groupBy("province")
      .pivot("rank", Array("1", "2", "3"))
      .agg(max("account_id"))
      .withColumnRenamed("1", "account_id_1")
      .withColumnRenamed("2", "account_id_2")
      .withColumnRenamed("3", "account_id_3")

    val loginTimePivotDF: DataFrame = loginRankDF.groupBy("province")
      .pivot("rank", Array("1", "2", "3"))
      .agg(max("logtime"))
      .withColumnRenamed("1", "login_time_1")
      .withColumnRenamed("2", "login_time_2")
      .withColumnRenamed("3", "login_time_3")

    // 落库
    val loginJoinDF: DataFrame = accountPivotDF.join(loginTimePivotDF, "province")
    // 列调整
    val resultDF: DataFrame = loginJoinDF.withColumn("province", $"province")
      .withColumn("account_id_1", $"account_id_1")
      .withColumn("login_time_1", $"login_time_1")
      .withColumn("account_id_2", $"account_id_2")
      .withColumn("login_time_2", $"login_time_2")
      .withColumn("account_id_3", $"account_id_3")
      .withColumn("login_time_3", $"login_time_3")

    resultDF.createTempView("result")

    sparkSession.sql(
      """
        |insert overwrite table login_time_province_top3
        |select province,account_id_1,login_time_1,account_id_2,login_time_2,account_id_3,login_time_3
        |from result
        |""".stripMargin
    )
  }

}
