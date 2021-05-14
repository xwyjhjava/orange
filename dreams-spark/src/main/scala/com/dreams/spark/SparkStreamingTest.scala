package com.dreams.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * @Package com.dreams.spark
 * @author ming
 * @date 2021/1/13 19:08
 * @version V1.0
 * @description TODO
 */
object SparkStreamingTest {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("spark streaming test")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = sparkSession.sqlContext

    val ssc = new StreamingContext(sc, Duration(2000))

    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.199.132", 8888)

    val dstream: DStream[String] = stream.flatMap(_.split(","))

    val result: DStream[String] = dstream.map(arr => {
      if (arr == "load") {
        arr
      }else{
        arr + "else"
      }
    })

    result.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
