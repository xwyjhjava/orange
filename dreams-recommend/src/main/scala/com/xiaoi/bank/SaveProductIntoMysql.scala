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
    val nameDF: DataFrame = df.map(row => {
      val name: String = row.getAs[String]("name")
      val prodName: String = name.replace("北部湾财产保险股份有限公司", "")

      var category = "其他"
      category = prodName match {
        case pName if (pName.contains("交通") && pName.contains("强制")) => "交强险"
        case pName if (pName.contains("商业") && pName.contains("机动车")) => "商业车险"
        case pName if (pName.contains("家庭") && pName.contains("财产")) => "家财险"
        case pName if pName.contains("船舶") => "船舶险"
        case pName if pName.contains("意外") => "意外险"
        case pName if pName.contains("责任") => "责任险"
        case pName if (pName.contains("货物运输") && !pName.contains("责任")) => "货运险"
        case pName if pName.contains("工程") => "工程险"
        case pName if (pName.contains("医疗") && !pName.contains("意外")) => "健康险"
        case pName if pName.contains("疾病") => "健康险"
        case pName if (pName.contains("种植") || pName.contains("养殖")) => "农业险"
        case pName if pName.contains("保证") => "保证险"
        case pName if pName.contains("退货运费") => "退货运费险"
        case pName if pName.contains("汽车修理") => "汽车修理厂综合保险"
        case _ => "其他"
      }

      (prodName, category)
    }).toDF("name", "category")

    nameDF.show()

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val url = "jdbc:mysql://172.16.20.11:3306/recommend_baoxian?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true"
    val table = "insurance_product"

    nameDF.write.mode(SaveMode.Append)
      .jdbc(url, table, properties)

    println("=========done================")









  }



}
