package com.xiaoi.bank

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/10 17:27
 * @version V1.0
 * @description TODO
 */
object SaveProductIntoMysql {


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)


  def main(args: Array[String]): Unit = {

    /**
     *
     */
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("save data")
      .master("local[*]")
      .getOrCreate()

    val sqlContext: SQLContext = sparkSession.sqlContext

//    sparkSession.read.format("jdbc")
//      .option("url", "")
//      .option("user", "")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("", "").load()


    val df: DataFrame = sparkSession.read
      .option("header", true)
      .csv("D:\\data\\bgic\\result.csv")

//    df.show()

    // 入库
    import sqlContext.implicits._
    val nameDF: Dataset[String] = df.map(row => {
      val name: String = row.getAs[String]("name")
      name.replace("北部湾财产保险股份有限公司", "")
    })
    nameDF.show()

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val url = "jdbc:mysql://172.16.20.23:3306/recommend_baoxian?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true"
    val table = "insurance_product"

    nameDF.write.mode(SaveMode.Overwrite)
      .jdbc(url, table, properties)

    println("=========done================")









  }



}
