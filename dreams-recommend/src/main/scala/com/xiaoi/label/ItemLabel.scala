package com.xiaoi.label

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * @Package com.xiaoi.label
 * @author ming
 * @date 2020/3/18 15:59
 * @version V1.0
 * @description TODO
 */
class ItemLabel(private val spark: SparkSession) extends Serializable {


  import spark.implicits._
  //构建item标签
  /*
  root
  |-- uid: integer (nullable = true)
  |-- sldate: timestamp (nullable = true)
  |-- userId: long (nullable = true)
  |-- itemId: integer (nullable = true)
  |-- itemName: string (nullable = true)
  |-- dptno: integer (nullable = true)
  |-- dptname: string (nullable = true)
  |-- bandno: integer (nullable = true)
  |-- bandname: string (nullable = true)
  |-- qty: double (nullable = true)
  |-- amt: double (nullable = true)
  |-- gender: integer (nullable = true)
  |-- birthday: timestamp (nullable = true)
  |-- createdate: timestamp (nullable = true)
  */
  def buildItemLabel(df: DataFrame, calendar_df: DataFrame): Unit ={



    // 大类
    val big_type = ""
    // 中类
    val middle_type = ""

    val itemRDD: RDD[(Integer, String, String)] = df.map(row => {
      val item_id: Integer = row.getAs[Integer]("itemId")
      val small_type_id: Integer = row.getAs[Integer]("dptno")
      val small_type_name: String = row.getAs[String]("dptname")
      // 拼接类别编码和类别名称作为小类标签的值
      val small_type = small_type_id + ":" + small_type_name

      val sldate: Timestamp = row.getAs[Timestamp]("sldate")

      (item_id, small_type, sldate.toString)
    }).rdd

    // 获取每一个item对用的购买时间
    val itemOrderDateMap: Map[Integer, String] = itemRDD.map(ele => {
      (ele._1, ele._3)
    }).collect().toMap

    // 获取日历Map
    val calendarMap: Map[String, Integer] = getCalendarMap(calendar_df)
    // broadcast calendarMap
    val calendarMapBroad: Broadcast[Map[String, Integer]] = spark.sparkContext.broadcast(calendarMap)

    val resultLabelRDD: RDD[(Integer, String)] = itemRDD.map(item => {
      val itemId: Integer = item._1
      //      val smallType: String = item._2
      val orderDate: String = item._3.split(" ")(0)
      val calendarMapVal = calendarMapBroad.value
      val orderType: Integer = calendarMapVal.get(orderDate).get

      (itemId, (orderDate, orderType))
    }).groupByKey()
      .map(ele => {

        val itemId: Integer = ele._1

        val value: Iterable[(String, Integer)] = ele._2
        val orderTypeList: List[(String, Integer)] = value.toList
        // 本商品的总交易量
        val itemTotalOrderSize: Float = orderTypeList.size

        var orderInWorkDayCount: Long = 0
        var orderInOffDayCount: Long = 0
        var orderInHolidayCount: Long = 0

        // 记录orderDate， 用于后续的节假日， 这里是节假日的最后一天
        var orderDate: String = ""
        var orderType: Integer = 0


        for (t <- orderTypeList) {

          orderDate = t._1
          orderType = t._2

          if (orderType == 0) { // workday
            orderInWorkDayCount = orderInWorkDayCount + 1
          } else if (orderType == 1) { // off day
            orderInOffDayCount = orderInOffDayCount + 1
          } else if (orderType == 2) { // holiday
            orderInHolidayCount = orderInHolidayCount + 1
          }
        }
        // 构建orderInDayMap
        val orderDayTypeMap = Map(
          orderInWorkDayCount -> "工作日畅销",
          orderInOffDayCount -> "休息日畅销",
          orderInHolidayCount -> ("节假日畅销" + orderDate)
        )

        // 比较 workday、offday、holiday销量， 得出最大值
        val maxOrderCount: Long = Math.max(Math.max(orderInWorkDayCount, orderInOffDayCount), orderInHolidayCount)

        // 最畅销ratio
        val maxOrderCountRatio: String = (maxOrderCount / itemTotalOrderSize).formatted("%.2f")

        // 得到标签
        val resultLabel: String = orderDayTypeMap.get(maxOrderCount).get + ":" + maxOrderCountRatio
        (itemId, resultLabel)
      })

      resultLabelRDD.take(10)
      .foreach(println)
  }


  def getWorkAndHolidayLabel(): Unit ={


  }


  def getCalendarMap(calendar_df: DataFrame):Map[String, Integer] ={
    val calendarMap: Map[String, Integer] = calendar_df.map(row => {
      val date: Timestamp = row.getAs[Timestamp]("date")
      val dateType: Integer = row.getAs[Integer]("type")
      (date.toString, dateType)
    }).rdd
      .collect()
      .toMap
    calendarMap
  }


}

object ItemLabel{

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder()
      .appName("item label")
      .master("local[*]")
      .getOrCreate()

    // 准备原始数据集
    val df: DataFrame = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("D:\\ming\\bigdata\\datasupermarket\\supermarket.csv")

    // 日历数据  (0: 工作日 1: 休息日  2: 节假日)
    val calendar_df: DataFrame = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("D:\\ming\\bigdata\\datasupermarket\\calendar.csv")

//    calendar_df.printSchema()
//    calendar_df.show(10)

//    import spark.implicits._
//
//    val calendar2018_df: DataFrame = spark.read
////      .option("inferSchema", true)
////      .option("header", true)
//      .textFile("D:\\ming\\bigdata\\datasupermarket\\2018")
//      .map(ele => {
//        val array: Array[String] = ele.split("\\|")
//        (array(0), array(1))
//      }).toDF("date", "type")
//
//
//    val calendar_df: Dataset[Row] = calendar2017_df.union(calendar2018_df).distinct()
//
//    val calendarSize: Long = calendar_df.count()
//    println("calendarSize => " + calendarSize)

//    calendar_df.toDF("date", "type")
//        .coalesce(1)
//        .write
//        .mode(SaveMode.Overwrite)
//        .option("header", true)
//        .csv("D:\\ming\\bigdata\\datasupermarket\\calendar")
//
//    println(" save end ")


    //    df.printSchema()

    println("===========================")

//    calendar_df.printSchema()


    val itemLabel = new ItemLabel(spark)
    itemLabel.buildItemLabel(df, calendar_df)

//    calendar_df.printSchema()
//    calendar_df.show(5)



  }

}
