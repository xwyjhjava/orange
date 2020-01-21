package com.dreams.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.Test

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

/**
 * @Package com.dreams.test
 * @author ming
 * @date 2020/1/14 16:55
 * @version V1.0
 * @description scala test main
 */
object TestMainScala {

  def main(args: Array[String]): Unit = {
//    run01()
//      testReadFile()
    saveCsvFile()
  }

  def run01(): Unit ={

    val spark: SparkSession = SparkSession.builder()
      .appName("run")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    val chat_level: RDD[String] = sc.makeRDD(Seq("abc|2"))
    val chatInfo: mutable.Map[String, Int] = collection.mutable.Map("" -> 0)
    chat_level.collect().toList.map(x => {
      val splits: Array[String] = x.trim.split("\\|", -1)
      val score: Int = splits(1).toInt
      val ques: String = splits(0).trim
      chatInfo += (ques -> score)
    })

    chatInfo.foreach(println)
  }

  def removeChar(raw: String , regex: String) = {
    val Pattern = s""".*([^pP${regex}]+).*""".r
    raw match {
      case Pattern(c) => raw.replaceAll(s"""[\\pP${regex}]+""", " ").trim
      case _ => raw
    }
  }


  def isIllegalChar(raw: String, regex: String) ={
    val Pattern = s""".*([^\\pP]+).*""".r
    raw match {
      case Pattern(c) => false
      case _ => true
    }
  }

  def testReadFile(): Unit ={
    // Source 类是scala.io 提供的； 内部实现是 new FileInputStream
    val source: BufferedSource = Source.fromFile("D:\\xiaoi\\rec_min.txt")
    val list: List[String] = source.getLines().toList
    list.take(2).foreach(println)
  }


  def saveCsvFile(): Unit ={

    val spark = SparkSession.builder()
      .appName("save file")
      .master("local[*]")
      .getOrCreate()

    val dataDF = spark.createDataFrame(Array(("1","2020-01-19 16:10:20", "1", "你好，请问这款理财产品的售后怎么处理？", "1", "微信")))
        .toDF("ID","create_date","period_type", "question", "cluster_id", "platform")
    dataDF.show()

    dataDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", true)
      .csv("D:\\xiaoi\\lp")

  }

}
