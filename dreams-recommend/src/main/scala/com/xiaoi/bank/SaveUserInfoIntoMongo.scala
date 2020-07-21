package com.xiaoi.bank

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/13 10:11
 * @version V1.0
 * @description TODO
 */
object SaveUserInfoIntoMongo {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {


    val read_config = ReadConfig(Map(
      "uri" -> "mongodb://meizu:Xi_aoi157=@122.226.240.157:20191/recommend",
      "collection" -> "user_profile"
    ))

    val writeConfig = WriteConfig(
      Map(
        "uri" -> "mongodb://meizu:Xi_aoi157=@122.226.240.157:20191/recommend",
        "collection" -> "mock_user_for_beibuwan"
      )
    )


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("mongo read and write")
      .master("local[*]")
      .getOrCreate()



    //读mock user
    val userDF: DataFrame = sparkSession.read
      .option("header", true)
      .load("D:\\data\\cmb\\userWithAllString")

    userDF.show()



    println("save userDF to mongo start")

    import sparkSession.sqlContext.implicits._

//    MongoSpark.save(userDF, writeConfig)

    import com.mongodb.spark._
    // todo distinct 不能省， 否则无法入库， 具体原因暂不清楚
    // 写157mongo, 连接不稳定
    userDF.rdd.distinct().map(row => {

      val doc = new Document()

      doc.put("ADR_ID", row.getAs[String]("ADR_ID"))
      doc.put("AGE", row.getAs[String]("AGE"))
      doc.put("APP_INC_COD", row.getAs[String]("APP_INC_COD"))
      doc.put("CC_COD", row.getAs[String]("CC_COD"))
      doc.put("CHILD_FLAG", row.getAs[String]("CHILD_FLAG"))
      doc.put("CITY_ID", row.getAs[String]("CITY_ID"))
      doc.put("CIT_COD", row.getAs[String]("CIT_COD"))
      doc.put("COR_TYPE", row.getAs[String]("COR_TYPE"))
      doc.put("EDU", row.getAs[String]("EDU"))
      doc.put("ETH_GRP", row.getAs[String]("ETH_GRP"))
      doc.put("HOS_STS", row.getAs[String]("HOS_STS"))
      doc.put("INC", row.getAs[String]("INC"))
      doc.put("MAR_STS", row.getAs[String]("MAR_STS"))
      doc.put("NBR", row.getAs[String]("NBR"))
      doc.put("OC_COD", row.getAs[String]("OC_COD"))
      doc.put("SEX", row.getAs[String]("SEX"))
      doc.put("VEH_FLG", row.getAs[String]("VEH_FLG"))
      doc
    }).saveToMongoDB(writeConfig)


  }



}
