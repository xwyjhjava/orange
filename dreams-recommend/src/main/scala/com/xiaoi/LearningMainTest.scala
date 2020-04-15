package com.xiaoi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
 * @Package com.xiaoi
 * @author ming
 * @date 2020/4/8 10:42
 * @version V1.0
 * @description 自学习测试用例
 */
object LearningMainTest {

  private val LOG_PATH = "E:\\xiaoi\\自学习\\06_1"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Learning platform test")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    getLogData(sc)

  }


  // load 日志数据
  def getLogData(sparkContext: SparkContext): Unit ={

    val count: Long = sparkContext.textFile("E:\\xiaoi\\自学习\\06_1")
      .count()
    println(count)


    sparkContext.textFile(LOG_PATH)
      .mapPartitions(iter => {
        val result = new ListBuffer[(String, String, String, String, Int)]
        while (iter.hasNext){
          val ele: String = iter.next()
          val array: Array[String] = ele.split("\\|")
          val length: Int = array.length
          result += ((array(0), array(1), array(2), array(3), length))
        }
        result.iterator
      })
      .take(10)
      .foreach(println)

  }


}
