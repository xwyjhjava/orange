package com.dreams.spark.sap

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.spark.sql.execution.RowDataSourceScanExec
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Problem_Spark_Test {


  def main(args: Array[String]): Unit = {


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("spark test")
      .master("local")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("Error")

    import sparkSession.sqlContext.implicits._

    // 1. Read the json file
    val firstDF: DataFrame = sparkSession.read.format("json")
      .load("/Users/mingzhao/sap_spark/sample_data.json")

    // jsonDF.show()

    // 2. Filter outer the data
    val secondDF: Dataset[Row] = firstDF.filter($"DataSource" === "c4c" && $"ObjectName" === "visit" && $"TenantID" === "5003")

    secondDF.createTempView("tb_sample")
    // 3. Add a new column named organizationRefId
    // Value of ObjectName + “_” + value of TenantID + “_” + value of organizerId


    secondDF.printSchema()

//    secondDF.explode($"RowData")
    sparkSession.sql("select DataSource,ObjectName,RowData.visitEOS.visitLists.visitPlans from tb_sample").show()


    // Male Fale   == > 0 , 1
    // age   =>  0, 1
    //

    org.apache.spark.sql.functions

    // shuffle
//    key1, value1, key2,value,




//    secondDF.toDF().rdd.map(row => {
//      val dataSource: String = row.getAs[String]("DataSource")
//      val objectName: String = row.getAs[String]("ObjectName")
//      val rowCount: String = row.getAs[String]("RowCount")
//      val rowData: String = row.getAs[String]("RowData")
//      val tenantId: String = row.getAs[String]("TenantID")
//
//      // 解析RowData
//      val jsonObj: JSONObject = JSON.parseObject(rowData)
//      val visitListJsonString: String = jsonObj.getString("visitLists")
//      val visitListJsonArray: JSONArray = JSON.parseArray(visitListJsonString)
//
//
//
//    })


  }


}
