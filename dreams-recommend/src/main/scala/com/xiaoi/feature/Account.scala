package com.xiaoi.feature

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Years}
import org.slf4j.LoggerFactory
import scopt.OptionParser

@Deprecated
object Account {


  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(param: Params): Unit ={

    val spark = SparkSession.builder()
          .appName("user_basic_feature")
          .master("local[*]")
          .getOrCreate()

    logger.info("account feature start")
//    val spark = new SparkInit().sparksession(false)

    import spark.sqlContext.implicits._
    val account_set = spark.read.textFile(param.credit_amount_input_path)
      .map(_.split("\\|"))
      .map(arr => Account(arr(0), arr(1), arr(2), arr(3), arr(4)))

    val fmt = DateTimeFormat.forPattern("YYYY-MM-dd")

    val processed_accountSet = account_set.collect().map(account => {
      //固定额度，单位：万元
      val amount = account.credit_amount.toDouble
      val amountType = amount match {
        case amount if amount > 0 && amount <= 1 => "1"
        case amount if amount > 1 && amount <= 5 => "2"
        case amount if amount > 5 && amount <= 10 => "3"
        case amount if amount > 10 => "4"
        case _ => "5"
      }

      //账户时长处理
      val date = fmt.parseDateTime(account.open_account_date)
      val now = DateTime.now()
      val interval_days =  Years.yearsBetween(date, now).getYears

      val duration = interval_days match {
          //账户小于一年
        case 0 => "1"
          //账户在1-3年
        case interval_days if interval_days > 0 && interval_days <= 3 => "2"
          //账户超过三年
        case interval_days if interval_days > 3 => "3"
        case _ => "4"
      }

      val count = account.card_count.toInt
      val card_count =  count match {
        case 1 => "1"
        case 2 => "2"
        case 3 => "3"
        case count if count > 3 => "4"
        case _ => "5"
      }

      Account(account.userId, amountType, duration, card_count, account.high_card_level)
    })

//    val stringColumns = Array("credit_amount", "open_account_date","card_count")

    val stringColumns = param.encode_field.split(",")

    if(stringColumns.length > 1){

      val account_set = spark.sparkContext.makeRDD(processed_accountSet).toDS()
        .select("*")

      val encoded_df = new BaseFun().oneHotEncode(account_set, stringColumns)
      val account_df = encoded_df.drop("credit_amount", "credit_amountIndex",
        "open_account_date", "open_account_dateIndex",
        "card_count", "card_countIndex", "userId")

      account_df.show()
    }else{
      val account_df = spark.sparkContext.makeRDD(processed_accountSet).toDS()
      account_df.show()
    }

//    val amount = account_df.select("credit_amountVec").schema.apply("credit_amountVec")
//    println(amount)
//    account_df.show()

  }


  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("accountFeature") {
      opt[String]("credit_amount_input_path")
        .action((x, c) => c.copy(credit_amount_input_path = x))
      opt[String]("encode_field")
        .action((x, c) => c.copy(encode_field = x))
      checkConfig { params =>
        success
      }
    }
    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }

  }


  case class Account(
                   userId: String,
                   credit_amount: String,
                   open_account_date: String,
                   card_count: String,
                   high_card_level: String
                   )

  case class Params(
                  credit_amount_input_path: String = "D:\\data\\mock_insurance\\credit_amount_mock.txt",
                  encode_field: String = ""
                  )


}
