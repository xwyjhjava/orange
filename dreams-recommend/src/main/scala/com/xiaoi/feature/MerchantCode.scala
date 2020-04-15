package com.xiaoi.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

/**
  * 分析商户号相关特征
  */
object MerchantCode {


  //构建特征字典
  // TODO: 后续改写成配置文件的方式
  val JEWELRY_CONSUME = Array("5094", "5932", "5937", "5944", "5950", "5970", "5971")
  val JOURNEY_CONSUME = Array("7011", "7012", "7032", "7033", "5948", "5972", "4411", "4457", "5309", "6051", "4722", "4733", "4511")
  val SPORTS_CONSUME = Array("7932", "7933", "7941", "7992", "7997", "5941")
  val CAR_CONSUME= Array("5532", "5533", "7512", "7513", "7531", "7534", "7535", "7538", "7542", "7549", "8675", "5541")
  val CHILD_CONSUME = Array("5611", "5641", "5945", "8351")
  val FAMILY_CONSUME = Array("5651", "5712", "5713", "5714", "5718", "5719", "7217", "7295")
  val MEDICAL_CONSUME = Array("5912", "5975", "8021", "8031", "8041", "8042", "8049", "8050", "8071", "8099")
  val EDUCATION_CONSUME = Array("8241", "8244", "8249", "8299")
  val INVESTMENT_CONSUME = Array("6211")
  val INSURANCE_CONSUME = Array("6300")

  //  import Array._
  //  val ALL_CONSUME = concat(JEWELRY_CONSUME, JOURNEY_CONSUME, SPORTS_CONSUME, CAR_CONSUME, CHILD_CONSUME, FAMILY_CONSUME,
  //    MEDICAL_CONSUME, EDUCATION_CONSUME, INVESTMENT_CONSUME, INSURANCE_CONSUME)


  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {

    logger.info("mcc feature start")
    val spark = SparkSession.builder()
      .appName("mcc_feature")
      .master("local[6]")
      .getOrCreate()

    //读取文件
    // (user_id, mcc_code)
    val mcc_input_path = params.mcc_input_path

    val df_user_mcc = spark.read
      .format("csv")
      .option("header", true)
      .load(mcc_input_path)

    val user_mcc_rdd = df_user_mcc.select("CLT_NBR", "MCH_NBR").rdd
      .map(row => (row.getAs[String]("CLT_NBR"), row.getAs[String]("MCH_NBR")))

    //根据特征字典构建mcc特征
    val user_feature = build_mcc_feature(user_mcc_rdd)

    //样式数据展示
    import spark.sqlContext.implicits._
    val user_feature_ds = user_feature.map(ele => user_mcc_feature(ele._1, ele._2, ele._3, ele._4, ele._5,
      ele._6, ele._7, ele._8, ele._9, ele._10)).toDS()
    user_feature_ds.show(10)

  }

  //根据特征字典构建mcc特征
  def build_mcc_feature(user_mcc: RDD[(String, String)]) ={

    val feature = user_mcc.map(ele => {
      //用户ID
      val user_id = ele._1
      //截取商户编码
      val mcc = ele._2.substring(7, 11)

      val feature_type = mcc match {
        case mcc if JEWELRY_CONSUME.exists(_.contains(mcc)) => "jewelry_consume"
        case mcc if JOURNEY_CONSUME.exists(_.contains(mcc)) => "journey_consume"
        case mcc if SPORTS_CONSUME.exists(_.contains(mcc)) => "sport_consume"
        case mcc if CAR_CONSUME.exists(_.contains(mcc)) => "car_consume"
        case mcc if CHILD_CONSUME.exists(_.contains(mcc)) => "child_consume"
        case mcc if MEDICAL_CONSUME.exists(_.contains(mcc)) => "medical_consume"
        case mcc if EDUCATION_CONSUME.exists(_.contains(mcc)) => "education_consume"
        case mcc if INVESTMENT_CONSUME.exists(_.contains(mcc)) => "investment_consume"
        case mcc if INSURANCE_CONSUME.exists(_.contains(mcc)) => "insurance_consume"
        case _ => "default"
      }

      (user_id, feature_type)

    })
    //二值法
    feature.groupByKey().map(ele => {
      var is_jewelry_consume = 0
      var is_journey_consume = 0
      var is_sport_consume = 0
      var is_car_consume = 0
      var is_child_consume = 0
      var is_medical_consume = 0
      var is_education_consume = 0
      var is_investment_consume = 0
      var is_insurance_consume = 0

      val type_array = new ArrayBuffer[String]()

      ele._2.foreach(x => type_array.append(x))

      if (type_array.contains("jewelry_consume")) is_jewelry_consume = 1
      if (type_array.contains("journey_consume"))  is_journey_consume = 1
      if (type_array.contains("sport_consume")) is_sport_consume = 1
      if (type_array.contains("car_consume"))  is_car_consume = 1
      if (type_array.contains("child_consume"))  is_child_consume = 1
      if (type_array.contains("medical_consume"))  is_medical_consume = 1
      if (type_array.contains("education_consume")) is_education_consume = 1
      if (type_array.contains("investment_consume")) is_investment_consume = 1
      if (type_array.contains("insurance_consume"))  is_insurance_consume = 1

      val user_id = ele._1

      (user_id, is_jewelry_consume, is_journey_consume, is_sport_consume, is_car_consume,
        is_child_consume, is_medical_consume, is_education_consume, is_investment_consume, is_insurance_consume)

    })



  }

  def agency_feature(agency_code: RDD[String]): Unit ={

    agency_code.map(x=> (x, 1)).reduceByKey(_+_)
      .sortBy(x=>x._2)

  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("mccFeature"){
      opt[String]("mcc_input_path")
        .action((x, c) => c.copy(mcc_input_path = x))
      checkConfig{ params =>
        success
      }
    }
    parser.parse(args, defaultParams).map{params =>
      run(params)
    }.getOrElse{
      sys.exit(1)
    }
  }



  case class user_mcc_feature(
                               user_id: String,
                               is_jewelry_consume: Int,     // is_jewelry_consume
                               is_journey_consume:Int,      // is_journey_consume
                               sport_consume:Int,      // sport_consume
                               car_consume:Int,      // car_consume
                               child_consume:Int,      // child_consume
                               medical_consume:Int,      //  medical_consume
                               education_consume:Int,      //  education_consume
                               investment_consume:Int,      //  investment_consume
                               insurance_consume:Int      //  insurance_consume
                             )

  case class Params(
                     mcc_input_path: String = "D:\\data\\mcc\\feature\\user_mcc"
                   )

}
