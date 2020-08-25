package com.dreams.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Package com.dreams.spark
 * @author ming
 * @date 2020/7/22 10:20
 * @version V1.0
 * @description spark3.0 的hello world
 */
object HelloWorld {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("hello world")
      .master("local[*]")
      .getOrCreate()

//    sparkSession.read.load("testfile\\helloworld.txt")

    val sc: SparkContext = sparkSession.sparkContext

    val dataRDD: RDD[(String, Int)] = sc.textFile("testfile\\helloworld.txt")
      .map(ele => (ele, 1))


    import sparkSession.sqlContext.implicits._
    val dataDF: DataFrame = dataRDD.toDF("txt", "count")

    dataDF.show()


  }


}
