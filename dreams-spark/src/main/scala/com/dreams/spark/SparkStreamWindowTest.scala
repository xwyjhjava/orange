package com.dreams.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Durations, StreamingContext}

/**
 * @Package com.dreams.spark
 * @author ming
 * @date 2020/11/10 17:03
 * @version V1.0
 * @description TODO
 */
object SparkStreamWindowTest {

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("spark streaming test")
      .master("local")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = sparkSession.sqlContext


    val ssc = new StreamingContext(sc, Duration(10))

    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.199.168", 8888)

    val ds: DStream[(String, Int)] = stream.map(x => {
      val arr: Array[String] = x.split(" ")
      (arr(0), arr(1))
    }).transform(rdd => {
      val newRDD: RDD[(String, String)] = rdd.distinct()
      newRDD.map(tp => {
        (tp._1, 1)
      })
    })


    val uv: DStream[(String, Int)] = ds.reduceByKeyAndWindow((v1:Int, v2:Int) => {
      v1 + v2
    }, Durations.seconds(20), Durations.seconds(10))

    uv.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
