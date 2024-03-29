package com.xiaoi.label

import java.sql.Timestamp
import java.util

import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.spark.config.WriteConfig
import com.xiaoi.config.CalendarConfig
import com.xiaoi.util.MongoUtil.mongoDBConn
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.bson.Document

import scala.collection.mutable.ListBuffer

/**
 * @Package com.xiaoi.label
 * @author ming
 * @date 2020/3/18 15:59
 * @version V1.0
 * @description 构建item标签， 标签存mongo
 *              数据结构如下：
 *              {
 *                item_id: "itemId",
 *                big_type: "大类",
 *                middle_type: "种类",
 *                small_type: "小类",
 *                sale_attr: {},
 *                labels: {},
 *                tags: {},
 *                keywords:{},
 *                url: ""
 *                description: ""  //描述，可以用来计算物品间相似度,
 *                expire_time: "",
 *                update_time: ""
 *              }
 */
class ItemLabel(private val spark: SparkSession) extends Serializable {


  private val WORK_AND_HOLIDAY_OUTPUT_PATH = "D:\\ming\\bigdata\\datasupermarket\\workAndHolidayLabel"
  private val HOLIDAY_LABEL_OUTPUT_PATH = "D:\\ming\\bigdata\\datasupermarket\\holidayLabel"
  private val TIMESTAMP_LABEL_OUTPUT_PATH = "D:\\ming\\bigdata\\datasupermarket\\timestampLabel"
  private val SEASON_LABEL_OUTPUT_PATH = "D:\\ming\\bigdata\\datasupermarket\\seasonLabel"
  private val SALE_LABEL_OUTPUT_PATH = "D:\\ming\\bigdata\\datasupermarket\\saleLabel"
  private val SAME_CATEGORY_OUTPUT_PATH = "D:\\ming\\bigdata\\datasupermarket\\sameCategoryLabel"


  private val mongoDBName = "recommend"
  private val mongoColName = "itemLabel"
  private val mongoUser = "xiaoi"
  private val mongoPwd = "xiaoi"
  private val mongoUriStr = s"mongodb://${mongoUser}:${mongoPwd}@127.0.0.1:27017/${mongoDBName}"
  private val (client, collection) = mongoDBConn(mongoUriStr, mongoDBName, mongoColName)


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



    // 调用标签生成的方法
    
    val writeConfig = WriteConfig(Map(
          "uri" -> "mongodb://xiaoi:xiaoi@127.0.0.1:27017/recommend",
          "collection" -> "itemLabel"
        ))

    val workAndHolidayRDD: RDD[(Integer, String)] = readLabelData(spark, WORK_AND_HOLIDAY_OUTPUT_PATH, "work_holiday_label")

    val holidayRDD: RDD[(Integer, String)] = readLabelData(spark, HOLIDAY_LABEL_OUTPUT_PATH, "holiday_label")

    val timestampRDD: RDD[(Integer, String)] = readLabelData(spark, TIMESTAMP_LABEL_OUTPUT_PATH, "timestamp_label")

    val seasonRDD: RDD[(Integer, String)] = readLabelData(spark, SEASON_LABEL_OUTPUT_PATH, "season_label")

    val saleRDD: RDD[(Integer, String)] = readLabelData(spark, SALE_LABEL_OUTPUT_PATH, "sale_label")

    val sameCategoryRDD: RDD[(Integer, String)] = readLabelData(spark, SAME_CATEGORY_OUTPUT_PATH, "same_category_label")


//    import com.mongodb.spark._
//    workAndHolidayRDD.map(ele => {
//      val doc = new Document()
//      doc.put("item_id", ele._1)
//      doc
//    })
//      .saveToMongoDB(writeConfig)


    println("---saving work and holiday---")
    saveLabelResultToMongo(workAndHolidayRDD, "工作日/节假日畅销")

    println("---saving holidayRDD--")
    saveLabelResultToMongo(holidayRDD, "节假日畅销")
//
    println("---saving timestampRDD---")
    saveLabelResultToMongo(timestampRDD, "时间段畅销")
//
    println("---saving seasonRDD---")
    saveLabelResultToMongo(seasonRDD, "季节畅销")
//
    println("---saving saleRDD---")
    saveLabelResultToMongo(saleRDD, "畅销度")
//
    println("---saving samecategoryRDD---")
    saveLabelResultToMongo(sameCategoryRDD, "同品类中排名")

  }

  /**
   *
   * @param labelRDD
   * @description 将标签存到mongo中
   *
   */
  private def saveLabelResultToMongo(labelRDD: RDD[(Integer, String)], labelName: String): Unit ={

    val bulkArray = new util.ArrayList[UpdateOneModel[Document]]()

    labelRDD.collect().foreach(ele => {

      val item_id: Integer = ele._1
      val label: String = ele._2
      val queryDoc = new Document("item_id", item_id)
      val updateDoc = new Document(labelName, label)
      val execDoc = new Document("$push", new Document("labels", updateDoc))

      val updateModel = new UpdateOneModel[Document](queryDoc, execDoc)
      bulkArray.add(updateModel)

      if(bulkArray.size() % 5000 == 0){
        val bulkWriteResult: BulkWriteResult = collection.bulkWrite(bulkArray)
        println("number_1: " + bulkArray.size())
        bulkArray.clear()
        println("number_2: " + bulkArray.size())
      }
    })
    println("size" + bulkArray.size())
    collection.bulkWrite(bulkArray)
  }





  /**
   *
   * @param df  data_clean data
   * @param calendar_df  日历data, 标注工作日、休息日、节假日（2017年和2018年）
   * @return  （item_id, work_holiday_label）
   */
  private def getWorkAndHolidayLabel(df: DataFrame, calendar_df: DataFrame) :DataFrame = {
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
          orderType = t._2
          if (orderType == 0) { // workday
            orderInWorkDayCount = orderInWorkDayCount + 1
          } else if (orderType == 1) { // off day
            orderInOffDayCount = orderInOffDayCount + 1
          } else if (orderType == 2) { // holiday
            orderInHolidayCount = orderInHolidayCount + 1
            orderDate = t._1
          }
        }
        // 构建orderInDayMap
        val orderDayTypeMap = Map(
          orderInWorkDayCount -> "工作日畅销",
          orderInOffDayCount -> "休息日畅销",
          orderInHolidayCount -> ("节假日畅销:" + orderDate)
        )

        // 比较 workday、offday、holiday销量， 得出最大值
        val maxOrderCount: Long = Math.max(Math.max(orderInWorkDayCount, orderInOffDayCount), orderInHolidayCount)

        // 最畅销ratio
        val maxOrderCountRatio: String = (maxOrderCount / itemTotalOrderSize).formatted("%.2f")

        // 得到标签
        val resultLabel: String = orderDayTypeMap.get(maxOrderCount).get + ":" + maxOrderCountRatio
        (itemId, resultLabel)
      })
    val resultDF: DataFrame = resultLabelRDD.toDF("item_id", "work_holiday_label")

    saveLabelResultToLocal(resultDF, WORK_AND_HOLIDAY_OUTPUT_PATH)

    resultDF
  }

  /**
   *
   * @param df  getWorkAndHoliday方法的返回值DataFrame
   * @return    （item_id， holidayLabel）
   * @description 得到节假日标签
   */
  private def getHolidayLabel(df: DataFrame): DataFrame ={
    // 根据上一个结果，结合节假日，进一步得到标签
    val holidayDF: DataFrame = df.select("item_id", "work_holiday_label")
      // 过滤出节假日畅销的item
      .filter($"work_holiday_label".contains("节假日"))
      .map(row => {
        val itemId = row.getAs[Integer]("item_id")
        // label数据格式为  节假日畅销：2018-10-04:0.5
        val holiday: String = row.getAs[String]("work_holiday_label")
        // 判断日期属于哪个节日
        val holidayDate: String = holiday.split(":")(1)
        // 得到ratio
        val holidayRatio: String = holiday.split(":")(2)
        // 得到具体节假日
        val holidayLabel: String = CalendarConfig.holidayMap.get(holidayDate).get

        (itemId, holidayLabel+"畅销")
      }).toDF("item_id", "holiday_label")

    saveLabelResultToLocal(holidayDF, HOLIDAY_LABEL_OUTPUT_PATH)

    holidayDF
  }


  /**
   *
   * @param df
   * @return
   * @description  计算时间段的标签
   */
  private def getTimestampLabel(df: DataFrame): DataFrame ={

    import org.apache.spark.sql.functions._

    val itemRDD: RDD[(Integer, Integer)] = df.select("itemId", "sldate")
      .withColumn("hour", hour($"sldate"))
      .map(row => {
        val itemId: Integer = row.getAs[Integer]("itemId")
        val hour: Integer = row.getAs[Integer]("hour")
        (itemId, hour)
      }).rdd

    /*
    早上 [5, 8)
    上午 [8, 11)
    中午 [11, 13)
    下午 [13, 18)
    晚上 [18, 23)
    */

    val timestampLabel: DataFrame = itemRDD.groupByKey()
      .map(ele => {

        val itemId: Integer = ele._1
        val hourList: List[Integer] = ele._2.toList
        // 本商品销售总和
        val itemTotalOrderSize: Float = hourList.size

        var earlymorning: Long = 0
        var morning: Long = 0
        var noon: Long = 0
        var afternoon: Long = 0
        var night: Long = 0


        // 根据时间段， 找出销量最好的时段
        for (hour <- hourList) {
          hour match {
            case hour if (hour >= 5 && hour < 8) => earlymorning = earlymorning + 1
            case hour if (hour >= 8 && hour < 11) => morning = morning + 1
            case hour if (hour >= 11 && hour < 13) => noon = noon + 1
            case hour if (hour >= 13 && hour < 18) => afternoon = afternoon + 1
            case hour if (hour >= 18 && hour < 23) => night = night + 1
          }
        }

        // 构建orderInTimestampMap
        val orderInTimestampMap = Map(
          earlymorning -> "早上畅销",
          morning -> "上午畅销",
          noon -> "中午畅销",
          afternoon -> "下午畅销",
          night -> "晚上畅销"
        )
        // 比较出最大值
        val max_count: Long = Math.max(night, Math.max(afternoon, Math.max(noon, Math.max(earlymorning, morning))))
        //ratio
        val max_ration: String = (max_count / itemTotalOrderSize).formatted("%.2f")
        val timestampLabel: String = orderInTimestampMap.get(max_count).get + ":" + max_ration
        (itemId, timestampLabel)

      }).toDF("item_id", "timestamp_label")


    saveLabelResultToLocal(timestampLabel, TIMESTAMP_LABEL_OUTPUT_PATH)

    timestampLabel
  }


  import org.apache.spark.sql.functions.month

  /**
   *
   * @param df  data_clean 数据
   * @return  （item_id, seasonLabel）
   * @description  季节畅销标签
   */
  private def getSeasonLabel(df: DataFrame): DataFrame ={

    val itemRDD: RDD[(Integer, Integer)] = df.select("itemId", "sldate")
      .withColumn("month", month($"sldate"))
      .map(row => {
        val itemId: Integer = row.getAs[Integer]("itemId")
        val month: Integer = row.getAs[Integer]("month")
        (itemId, month)
      }).rdd

    val seasonLabel: DataFrame = itemRDD.groupByKey()
      .map(ele => {

        val itemId: Integer = ele._1
        val seasonList: List[Integer] = ele._2.toList
        // 本商品销售总和
        val itemTotalOrderSize: Float = seasonList.size

        var spring: Long = 0
        var summer: Long = 0
        var autumn: Long = 0
        var winter: Long = 0


//        3-5月春 6-8月夏 9-11月秋 12-2月冬

        // 根据月份， 找出销量最好的季节
        for (season <- seasonList) {
          season match {
            case hour if (hour >= 3 && hour <= 5) => spring = spring + 1
            case hour if (hour >= 6 && hour <= 8) => summer = summer + 1
            case hour if (hour >= 9 && hour <= 11) => autumn = autumn + 1
            case hour if (hour == 12 || hour == 1 || hour == 2) => winter = winter + 1
          }
        }

        // 构建orderInTimestampMap
        val orderInSeasonMap = Map(
          spring -> "春天畅销",
          summer -> "夏天畅销",
          autumn -> "秋天畅销",
          winter -> "冬天畅销"
        )
        // 比较出最大值
        val max_count: Long = Math.max(winter, Math.max(autumn, Math.max(spring, summer)))
        //ratio
        val max_ration: String = (max_count / itemTotalOrderSize).formatted("%.2f")
        val seasonLabel: String = orderInSeasonMap.get(max_count).get + ":" + max_ration
        (itemId, seasonLabel)

      }).toDF("item_id", "season_label")


    saveLabelResultToLocal(seasonLabel, SEASON_LABEL_OUTPUT_PATH)

    seasonLabel

  }


  import org.apache.spark.sql.functions._
  /**
   *
   *
   * @param df  data_clean 数据
   * @return    （item_id, saleDegreeLabel）
   * @description 畅销度标签
   */
  private def getSaleDegreeLabel(df: DataFrame): DataFrame ={

//    val itemRDD: RDD[(Integer, Integer)] = df.select("itemId")
//      .withColumn("number", lit(1))
//      .map(row => {
//        val itemId: Integer = row.getAs[Integer]("itemId")
//        val number: Integer = row.getAs[Integer]("number")
//        (itemId, number)
//      }).rdd
//
//    itemRDD
//      .reduceByKey(_+_)
//      .sortByKey(false)


    val itemRDD: RDD[(Integer, Long)] = df.select("itemId", "userId")
      .groupBy("itemId")
      .agg(
        count("userId").as("count")
      )
      .sort(desc("count"))
      .map(row => {
        val itemId: Integer = row.getAs[Integer]("itemId")
        val totalCount: Long = row.getAs[Long]("count")
        (itemId, totalCount)
      }).rdd


    val itemWithRankRDD: RDD[(Integer, Long)] = itemRDD.zipWithIndex().map(ele => {
      val itemId: Integer = ele._1._1
      val rankIndex: Long = ele._2 + 1
      (itemId, rankIndex)
    })


    // 总的item个数
    val itemCount: Long = itemRDD.count()

    // 销量从小到大排序标记rank值， 取前1%作为 极热销
//                                  取前10%作为 热销
//                                  取10%-80%作为普通
//                                  取80%-99%作为冷门
//                                  最后1%作为极冷门

    val verHotIndex: Long = Math.round(itemCount * 0.01)
    val hotIndex: Long = Math.round(itemCount * 0.1)
    val normalIndex: Long = Math.round(itemCount * 0.8)
    val tailIndex: Long = Math.round(itemCount * 0.99)
    val veryTailIndex: Long = itemCount

//    175---1749---13991---17314---17489
    println(verHotIndex + "---" + hotIndex + "---" + normalIndex + "---" + tailIndex + "---" + veryTailIndex)

    val saleLabelDF: DataFrame = itemWithRankRDD.map(ele => {

      val itemId: Integer = ele._1
      val rankIndex: Long = ele._2

      var saleLabel = ""

      if(1 <= rankIndex && rankIndex < verHotIndex){
        saleLabel = "极热销"
      }
      if(verHotIndex <= rankIndex && rankIndex < hotIndex){
        saleLabel = "热销"
      }
      if(hotIndex <= rankIndex && rankIndex < normalIndex){
        saleLabel = "普通"
      }
      if(normalIndex <= rankIndex && rankIndex < tailIndex){
        saleLabel = "冷门"
      }
      if(tailIndex <= rankIndex && rankIndex <= veryTailIndex){
        saleLabel = "极冷门"
      }

//      rankIndex match {
//        case rankIndex if (1 <= rankIndex && rankIndex < verHotIndex) => saleLabel = "极热销"
//        case rankIndex if (verHotIndex <= rankIndex && rankIndex < hotIndex) => saleLabel = "热销"
//        case rankIndex if (hotIndex <= rankIndex && rankIndex < normalIndex) => saleLabel = "普通"
//        case rankIndex if (normalIndex <= rankIndex && rankIndex < tailIndex) => saleLabel = "冷门"
//        case rankIndex if (tailIndex <= rankIndex && rankIndex < veryTailIndex) => saleLabel = "极冷门"
//      }

      (itemId, saleLabel)
    }).toDF("item_id", "sale_label")


    saveLabelResultToLocal(saleLabelDF, SALE_LABEL_OUTPUT_PATH)

    saleLabelDF
  }


  /**
   *
   * @param df  data_clean 数据
   * @return (item_id, sameCategory_label)
   * @description  同品类排名
   */
  private def getSameCategoryLabel(df: DataFrame): DataFrame ={


    val itemRDD: RDD[(Integer, Integer)] = df.select("itemId", "dptno")
      .map(row => {
        val itemId: Integer = row.getAs[Integer]("itemId")
        val categoryId: Integer = row.getAs[Integer]("dptno")
        (categoryId, itemId)
      }).rdd


    val resultLabelDF: DataFrame = itemRDD.groupByKey()
      // 过滤品类下小于三笔成交量的
      .filter(_._2.size > 2)
      .map(ele => {

        val itemList: List[Integer] = ele._2.toList

        val sortedItemList: List[(Integer, Int)] = itemList.map((_, 1))
          .groupBy(_._1)
          .mapValues(x => {
            // 统计每个item的销售量
            x.size
          })
          .map(item => {
            val itmId: Integer = item._1
            val count: Int = item._2
            (itmId, count)
          })
          .toList
          // 得到item销量的降序rank
          .sortWith(_._2 > _._2)

        // rank == 1 ， 排名靠前
        // rank == totalSize , 排名靠后
        // rank == 其他，  排名居中

        val listBuffer = new ListBuffer[String]

        //同品类排名靠前
        val headRankItemId: Integer = sortedItemList.take(1)(0)._1
        listBuffer.append(headRankItemId + ":同品类排名靠前")
        //同品类排名靠后
        val tailRankItemId: Integer = sortedItemList.takeRight(1)(0)._1
        listBuffer.append(tailRankItemId + ":同品类排名靠后")
        // 同品类排名居中

        sortedItemList.filter(x => {
          x._1 != headRankItemId && x._1 != tailRankItemId
        })
          .foreach(ele => {
            listBuffer.append(ele._1 + ":同品类排名居中")
          })

        listBuffer.toList

      })
      .flatMap(e => {e})
      .map(e => {
        val array: Array[String] = e.split(":")
        val itemId: String = array(0)
        val sameCategoryLabel: String = array(1)
        (itemId, sameCategoryLabel)
      })
      .toDF("item_id", "same_category_label")


    resultLabelDF.show(10)

    resultLabelDF.coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .option("header", true)
        .csv(SAME_CATEGORY_OUTPUT_PATH)

    
    
    
    resultLabelDF
    
    
  }

  // TODO: 周销售趋势

  // TODO: 月销售趋势

  // TODO: 复购周期



  // 销售标签

  // TODO: 颜色偏好


  // TODO: 物品档次


  // TODO: 价格水平


  // TODO: 场景标签


  /**
   *
   * @param calendar_df
   * @return
   * @description 建构calendar Map
   */
  private def getCalendarMap(calendar_df: DataFrame):Map[String, Integer] ={
    val calendarMap: Map[String, Integer] = calendar_df.map(row => {
      val date: Timestamp = row.getAs[Timestamp]("date")
      val dateKey: String = date.toString.split(" ")(0)
      val dateType: Integer = row.getAs[Integer]("type")
      (dateKey, dateType)
    }).rdd
      .collect()
      .toMap
    calendarMap
  }

  /**
   *
   * @param spark
   * @param input_path
   * @return
   * @description 读取中间结果
   */

  private def readLabelData(spark: SparkSession,
                            input_path: String,
                            labelField: String): RDD[(Integer, String)] ={

    val rdd: RDD[(Integer, String)] = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(input_path)
      .map(row => {
        val item_id: Integer = row.getAs[Integer]("item_id")
        val label: String = row.getAs[String](labelField)
        (item_id, label)
      }).rdd
    rdd
  }


  /**
   *
   * @param dataFrame
   * @param save_path
   * @description 保存label的dataframe
   */
  private def saveLabelResultToLocal(dataFrame: DataFrame, save_path: String): Unit ={

    dataFrame.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .csv(save_path)
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
