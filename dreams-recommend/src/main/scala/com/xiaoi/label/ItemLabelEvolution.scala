package com.xiaoi.label

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Package com.xiaoi.label
 * @author ming
 * @date 2020/4/20 11:10
 * @version V1.0
 * @description item Label 改进
 */
object ItemLabelEvolution {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)


  private def run(): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("item label evolution")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://meizu:Xi_aoi157=@122.226.240.157:20191/recommend.ui_action")
      .getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext

    // 准备item数据
    import com.mongodb.spark._

    val actionDF: DataFrame = MongoSpark.load(sparkSession)

    import sparkSession.implicits._
    val actionRDD: RDD[(String, String)] = actionDF.select("item_id", "order_date").map(row => {
      val orderDate: String = row.getAs[String]("order_date")
      val itemId: String = row.getAs[String]("item_id")
      (itemId, orderDate)
    }).rdd

    // 计算总的 log_count
    val totalLogCount: Long = actionRDD.count()


    // TODO: 改进标签的生成方式






    actionDF.show(10)



  }



  def main(args: Array[String]): Unit = {
    run()
  }


}
