package com.xiaoi.bank

import com.mongodb.spark.config.WriteConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/16 11:50
 * @version V1.0
 * @description TODO
 */
object SaveOrderIntoMongo {


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("mongo read and write")
      .master("local[*]")
      .getOrCreate()


    val writeConfig = WriteConfig(
      Map(
        "uri" -> "mongodb://meizu:Xi_aoi157=@122.226.240.157:20191/recommend",
        "collection" -> "mock_order_for_beibuwan"
      )
    )


    val orderDF: DataFrame = sparkSession.read
      .option("header", true)
      .load("D:\\data\\cmb\\order")

    orderDF.select("PROD_NO").distinct().show()


    // 写157mongo, 连接不稳定
    import com.mongodb.spark._
    orderDF.rdd.distinct().map(row => {

      val doc = new Document()
      doc.put("COVERAGE", row.getAs[String]("COVERAGE"))
      doc.put("INSURE_AGE", row.getAs[String]("INSURE_AGE"))
      doc.put("INSURE_TERM", row.getAs[String]("INSURE_TERM"))
      doc.put("INSURE_TERM_TY", row.getAs[String]("INSURE_TERM_TY"))
      doc.put("MERCH_COD", row.getAs[String]("MERCH_COD"))
      doc.put("MERCH_NAME", row.getAs[String]("MERCH_NAME"))
      doc.put("METHDD_TERM", row.getAs[String]("METHDD_TERM"))
      doc.put("METHDD_TERM_TY", row.getAs[String]("METHDD_TERM_TY"))
      doc.put("NBR", row.getAs[String]("NBR"))
      doc.put("PAYED_INSURED_SAME_F", row.getAs[String]("PAYED_INSURED_SAME_F"))
      doc.put("PAYT_PERQ", row.getAs[String]("PAYT_PERQ"))
      doc.put("PROD_NO", row.getAs[String]("PROD_NO"))
      doc.put("SALE_DATE", row.getAs[String]("SALE_DATE"))
      doc.put("TOT_FEE", row.getAs[String]("TOT_FEE"))
      doc.put("TYPE", row.getAs[String]("TYPE"))
      doc.put("YEAR_FEE", row.getAs[String]("YEAR_FEE"))

      doc

    }).saveToMongoDB(writeConfig)

  }

}
