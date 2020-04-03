package com.dreams.toutiao.analysis

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @Package com.dreams.toutiao.analysis
 * @author ming
 * @date 2020/4/3 16:04
 * @version V1.0
 * @description 内容分析
 */
object ContentAnalysis {




  private def readContentFile(sc: SparkContext): Unit ={



  }





  def main(args: Array[String]): Unit = {
//    过滤掉一些info日志
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

//    创建sparkSession
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("Content Analysis")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    //读取文件
    readContentFile(sc)



  }









}
