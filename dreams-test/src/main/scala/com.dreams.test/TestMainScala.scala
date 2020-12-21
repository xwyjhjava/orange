package com.dreams.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.util.Random

/**
 * @Package com.dreams.test
 * @author ming
 * @date 2020/1/14 16:55
 * @version V1.0
 * @description scala test main
 */
object TestMainScala {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("apache").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
//    run01()
//      testReadFile()
//    saveCsvFile()
//    contextSaveTest()
//    logDataCompare()
    isHit()
  }

  def run01(): Unit ={

    val spark: SparkSession = SparkSession.builder()
      .appName("run")
      .master("local[2]")
      .getOrCreate()
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


  def randomTest(): Unit ={

    val random = new Random()


    val spark = SparkSession.builder()
      .appName("save file")
      .master("local[*]")
      .getOrCreate()


    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[(String, String)] = sc.makeRDD(Seq(
      ("A", "1"),
      ("A", "2")
    ))

    val new_rdd: RDD[(String, String)] = rdd.map(x => {
      val new_key: String = x._1 + "_" + random.nextInt(2)
      (new_key, x._2)
    })

    new_rdd.foreach(println)

  }


  def contextSaveTest(): Unit ={


    val PATH = "D:\\data\\bgic\\result.csv"

    val spark = SparkSession.builder()
      .appName("save file")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val sc: SparkContext = spark.sparkContext
    val dataRDD: RDD[(String, String)] = sc.textFile("D:\\pyworkspace\\bank\\bgic\\result.txt")
      .map(_.split("\\|"))
      .filter(_.length > 1)
      .map(arr => {
        val text: String = arr(0)
        val link: String = arr(1)
        (text, link)
      })

//    dataRDD.repartition(1).saveAsTextFile(PATH)

    val dataFrame: DataFrame = dataRDD.toDF("name", "link")
//    dataFrame.coalesce(1).write
//      .mode(SaveMode.Overwrite)
//      .csv(PATH)



    dataFrame.coalesce(1).write
      .option("header", true)
      .csv("D:\\data\\bgic\\result.csv")


    val frame: DataFrame = spark.read.option("header", true)
      .csv(PATH)

    frame.show()



  }


  def logDataCompare(): Unit ={

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("log data compare")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    import sparkSession.sqlContext.implicits._

    val lessDataPath = "testfile\\log_data\\less\\23"
    val moreDataPath = "testfile\\log_data\\more\\23"

    val lessDF: DataFrame = sc.textFile(lessDataPath).map(_.split("\\|"))
      .map(arr => (arr(1), arr(2)))
      .toDF("sid", "uid")

    val moreDF: DataFrame = sc.textFile(moreDataPath).map(_.split("\\|"))
      .map(arr => (arr(1), arr(2)))
      .toDF("sid", "uid")


    val exceptDF: Dataset[Row] = moreDF.except(lessDF)
    exceptDF.show()


  }

  def tmpTest(): Unit ={

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()


    val sc: SparkContext = sparkSession.sparkContext

    val baseRDD: RDD[Array[String]] = sc.textFile("").map(_.split("\\|"))

    val array: Array[String] = baseRDD.map(arr => arr(3)).repartition(1).take(2000)
    val top_100: RDD[String] = sc.makeRDD(array)

    import sparkSession.sqlContext.implicits._
    import org.apache.spark.sql.functions._


    top_100.map(ele => ele)

    top_100.toDF("aa").groupBy("aa")
      .agg(
        count("aa").as("count")
      )

  }


  /**
   * 将全量数据以2000为一组，划分数据集（tensorflow全量会报错, 所以需要进行分批处理）
   */
  def testSplitData(): Unit ={

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = sparkSession.sqlContext

    import sqlContext.implicits._

    val baseRDD: RDD[Array[String]] = sc.textFile("file:///xiaoi/xx").map(_.split("\\|"))

    val quesIndexRDD: RDD[((String, String), Long)] = baseRDD.map(arr => (arr(3), arr(arr.length - 1))).distinct().zipWithIndex()

    // 验证数据条数
    quesIndexRDD.count()

    // 划分
    quesIndexRDD.filter(ele => {ele._2 >=0 && ele._2 < 2000}).map(_._1._1).repartition(1).saveAsTextFile("file:///xiaoi/emotion_model/test_data/origin_data_12/part01")
    quesIndexRDD.filter(ele => {ele._2 >=2000 && ele._2 < 4000}).map(_._1._1).repartition(1).saveAsTextFile("file:///xiaoi/emotion_model/test_data/origin_data_12/part02")
    quesIndexRDD.filter(ele => {ele._2 >=4000 && ele._2 < 6000}).map(_._1._1).repartition(1).saveAsTextFile("file:///xiaoi/emotion_model/test_data/origin_data_12/part03")
    quesIndexRDD.filter(ele => {ele._2 >=6000 && ele._2 < 8000}).map(_._1._1).repartition(1).saveAsTextFile("file:///xiaoi/emotion_model/test_data/origin_data_12/part04")
    quesIndexRDD.filter(ele => {ele._2 >=8000 && ele._2 < 10000}).map(_._1._1).repartition(1).saveAsTextFile("file:///xiaoi/emotion_model/test_data/origin_data_12/part05")
    quesIndexRDD.filter(ele => {ele._2 >=10000 && ele._2 < 12000}).map(_._1._1).repartition(1).saveAsTextFile("file:///xiaoi/emotion_model/test_data/origin_data_12/part06")
    quesIndexRDD.filter(ele => {ele._2 >=12000}).map(_._1._1).repartition(1).saveAsTextFile("file:///xiaoi/emotion_model/test_data/origin_data_12/part07")



    quesIndexRDD.filter(_._2 == 1999)


  }

  def isHit(): Unit ={

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = sparkSession.sqlContext


    val positiveArray: Array[String] = sc.textFile("E:\\xiaoi\\emotion_data\\positive.txt").collect()

    val resultRDD: RDD[String] = sc.textFile("E:\\xiaoi\\emotion_data\\ques2score")
      .map(_.split("\\|")).map(arr => arr(0))


    positiveArray.take(5).foreach(println)

    println("===========================")

    resultRDD.take(5).foreach(println)


    val hitRDD: RDD[String] = resultRDD.filter(e => {

      for (elem <- positiveArray) {
        if (e.contains(elem)) {
          true
        }
      }
      false
    })

    val count: Long = hitRDD.count()
    println("count:" + count)

    hitRDD.take(10).foreach(println)



  }


  def sortTest(): Unit ={




  }



}
