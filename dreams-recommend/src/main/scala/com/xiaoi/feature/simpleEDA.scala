package com.xiaoi.feature

import com.xiaoi.common.DateUtil.dateTimeDiffInDay
import com.xiaoi.common.{FrequencyUtil, HadoopOpsUtil, StatsUtil}
import com.xiaoi.constant.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/*
 *  对应以前的step_4   商品价格的统计值, 商品购买频次, 用户消费额的统计值 ,用户购物频次 ,
 *  用户购物花费（购物篮花费）,用户最近在item上购物次数 
 * */

object simpleEDA {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val now_date = params.now_date
    val conf = new SparkConf().setAppName("simpleEDA or step_4")
    val sc = new SparkContext(conf)
    remove_output_dir(params)

    logger.info("step_4 中间数据生成..(simpleEDA)....")
    val data_simplified = read_data_simplified(sc, params.data_simplified_input_path)
    val user_basket_money = read_user_basket_money(sc, params.user_basket_money_input_path)

    //计算商品单价的sta
    logger.info("item_price_sta saving......")
    val item_price_sta = get_item_price_sta(data_simplified)
    item_price_sta.repartition(1).saveAsTextFile(params.item_price_sta_output_path)

    //商品，距离观察点时间段中的交易记录数
    val item_frequency = get_item_frequency(data_simplified, now_date)

    logger.info("item_frequency_year saving......")
    val item_frequency_year = get_item_frequency_year(item_frequency)
    item_frequency_year.repartition(1)
      .saveAsTextFile(params.item_frequency_year_output_path)

    logger.info("item_frequency_month saving......")
    val item_frequency_month = get_item_frequency_month(item_frequency)
    item_frequency_month.repartition(1)
      .saveAsTextFile(params.item_frequency_month_output_path)

    logger.info("item_frequency_day saving......")
    val item_frequency_day = get_item_frequency_day(item_frequency)
    item_frequency_day.repartition(1)
      .saveAsTextFile(params.item_frequency_day_output_path)

    logger.info("user_basket_money_sta saving......")
    val user_basket_money_sta = get_user_basket_money_sta(user_basket_money)
    user_basket_money_sta.repartition(1)
      .saveAsTextFile(params.user_basket_money_sta_output_path)

    logger.info("user_log_money_sta saving......")
    val user_log_money_sta = get_user_log_money_sta(user_basket_money)
    user_log_money_sta.repartition(1)
      .saveAsTextFile(params.user_log_money_sta_output_path)


    //用户  每一条记录据观察点的时间段中的交易记录数
    val user_frequency = get_user_frequency(user_basket_money, now_date)

    logger.info("user_frequency_year saving......")
    val user_frequency_year = get_user_frequency_year(user_frequency)
    user_frequency_year.repartition(1)
      .saveAsTextFile(params.user_frequency_year_output_path)


    logger.info("user_frequency_month saving......")
    val user_frequency_month = get_user_frequency_month(user_frequency)
    user_frequency_month.repartition(1)
      .saveAsTextFile(params.user_frequency_month_output_path)

    logger.info("user_frequency_day saving......")
    val user_frequency_day = get_user_frequency_day(user_frequency)
    user_frequency_day.repartition(1)
      .saveAsTextFile(params.user_frequency_day_output_path)


    //用户  每一条记录据当前观察点的时间段中, 每一次购物的消费金额
    val user_money = get_user_money(user_basket_money, now_date)

    logger.info("user_money_year saving......")
    val user_money_year = get_user_money_year(user_money)
    user_money_year.repartition(1).saveAsTextFile(params.user_money_year_output_path)

    logger.info("user_money_month saving......")
    val user_money_month = get_user_money_month(user_money)
    user_money_month.repartition(1).saveAsTextFile(params.user_money_month_output_path)

    logger.info("user_money_day saving......")
    val user_money_day = get_user_money_day(user_money)
    user_money_day.repartition(1).saveAsTextFile(params.user_money_day_output_path)

    //该用户，该商品，距离观察点的时间
    val user_item_between_days = get_user_item_between_days(data_simplified, now_date)

    logger.info("user_item_frequency_month saving......")
    val user_item_frequency_month = get_user_item_frequency_month(user_item_between_days)
    user_item_frequency_month.repartition(1)
      .saveAsTextFile(params.user_item_frequency_month_output_path)

    logger.info("user_item_frequency_day saving......")
    val user_item_frequency_day = get_user_item_frequency_day(user_item_between_days)
    user_item_frequency_day.repartition(1)
      .saveAsTextFile(params.user_item_frequency_day_output_path)

    sc.stop()
  }

  //删除已存在的文件夹
  def remove_output_dir(params: Params) = {
    if (params.is_save) {
      HadoopOpsUtil.removeDir(params.item_price_sta_output_path,
        params.item_price_sta_output_path)
      HadoopOpsUtil.removeDir(params.item_frequency_year_output_path,
        params.item_frequency_year_output_path)
      HadoopOpsUtil.removeDir(params.item_frequency_month_output_path,
        params.item_frequency_month_output_path)
      HadoopOpsUtil.removeDir(params.item_frequency_day_output_path,
        params.item_frequency_day_output_path)
      HadoopOpsUtil.removeDir(params.user_basket_money_sta_output_path,
        params.user_basket_money_sta_output_path)
      HadoopOpsUtil.removeDir(params.user_log_money_sta_output_path,
        params.user_log_money_sta_output_path)
      HadoopOpsUtil.removeDir(params.user_frequency_year_output_path,
        params.user_frequency_year_output_path)
      HadoopOpsUtil.removeDir(params.user_frequency_month_output_path,
        params.user_frequency_month_output_path)
      HadoopOpsUtil.removeDir(params.user_frequency_day_output_path,
        params.user_frequency_day_output_path)
      HadoopOpsUtil.removeDir(params.user_money_year_output_path,
        params.user_money_year_output_path)
      HadoopOpsUtil.removeDir(params.user_money_month_output_path,
        params.user_money_month_output_path)
      HadoopOpsUtil.removeDir(params.user_money_day_output_path,
        params.user_money_day_output_path)
      HadoopOpsUtil.removeDir(params.user_item_frequency_month_output_path,
        params.user_item_frequency_month_output_path)
      HadoopOpsUtil.removeDir(params.user_item_frequency_day_output_path,
        params.user_item_frequency_day_output_path)
    } else {
      logger.info("无需保存...")
    }
  }

  //读取step_3_11_data_simplified文件,uid, sldat, user_id，item_id，dptno, qty, amt,
  // in_basket_pos, in_user_pos
  def read_data_simplified(sc: SparkContext, step_3_11_data_simplified: String):
      RDD[(String, String, String, String, String, String, String)] = {
    val data_simplified = sc.textFile(step_3_11_data_simplified).map(line => {
      val strArr = line.split("\\|", -1)
      val uid = strArr(0)
      val sldat = strArr(1)
      val user_id = strArr(2)
      val item_id = strArr(3)
      val dptno = strArr(4)
      val qty = strArr(5)
      val amt = strArr(6)
      //      val in_basket_pos = strArr(7)
      //      val in_user_pos = strArr(8)
      (uid, sldat, user_id, item_id, dptno, qty, amt)
    }).filter(x => {
      !x._3.equals("")
    })
    data_simplified
  }

  //读取step_3_22_user_basket_money文件,user_id, uid, sldat, basket_money, basket_size
  def read_user_basket_money(sc: SparkContext, user_basket_money_path: String):
      RDD[(String, String, String, String, String)] = {
    val user_basket_money = sc.textFile(user_basket_money_path).map(line => {
      val strArr = line.split("\\|", -1)
      val user_id = strArr(0)
      val uid = strArr(1)
      val sldat = strArr(2)
      val basket_money = strArr(3)
      val basket_size = strArr(4)
      (user_id, uid, sldat, basket_money, basket_size)
    })
    user_basket_money
  }

  //商品id,最小值、最大值、中位数、平均值，标准差
  def get_item_price_sta(data_simplified: RDD
      [(String, String, String, String, String, String, String)]): RDD[String] = {
    val item_price_sta = data_simplified.filter(x => {
      !x._6.equals("0")
    }).map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        (item_id, amt.toDouble / qty.toDouble)
      }.groupByKey().map(x => {
      val item_id = x._1
      val avg_all = x._2.toArray
      item_id + "|" + StatsUtil.sta_Count(avg_all)
    })
    item_price_sta
  }

  def get_item_frequency(data_simplified: RDD[(String, String, String, String, String, String, String)],
                         now_date: String): RDD[(String, (String, String, String, String))] = {
    val item_frequency = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        (item_id, dateTimeDiffInDay(sldat, now_date).toInt)
      }
      .filter(x => x._2 <= Constants.YEAR_CHOOSE)
      .groupByKey().map(x => {
      val item_id = x._1
      val sldat_Array = x._2.toList
      val year_month_day_count = FrequencyUtil.frequecy_count(Constants.ONE_MONTH, sldat_Array)
      val frequency_year = year_month_day_count._1.mkString("|")
      val frequency_month = year_month_day_count._2.mkString("|")
      val frequency_day = year_month_day_count._3
      val days_sta = StatsUtil.sta_Count(frequency_day.toArray)
      (item_id, (frequency_year, frequency_month, frequency_day.mkString("|"), days_sta))
    })
    item_frequency
  }

  def get_item_frequency_year(item_frequency: RDD[(String, (String, String, String, String))]):
      RDD[String] = {
    val item_frequency_year = item_frequency
      .map { case (item_id, (frequency_year, frequency_month, frequency_day, days_sta)) =>
        item_id + "|" + frequency_year
      }
    item_frequency_year
  }

  def get_item_frequency_month(item_frequency: RDD[(String, (String, String, String, String))]):
      RDD[String] = {
    item_frequency
      .map { case (item_id, (frequency_year, frequency_month, frequency_day, days_sta)) =>
        item_id + "|" + frequency_month
      }
  }

  def get_item_frequency_day(item_frequency: RDD[(String, (String, String, String, String))]):
      RDD[String] = {
    item_frequency
      .map { case (item_id, (frequency_year, frequency_month, frequency_day, days_sta)) =>
        item_id + "|" + frequency_day + "|" + days_sta
      }

  }

  def get_user_basket_money_sta(user_basket_money: RDD[(String, String, String, String, String)]):
      RDD[String] = {
    val user_basket_money_sta = user_basket_money.map {
      case (user_id, uid, sldat, basket_money, basket_size) => (user_id, basket_money.toDouble)
    }
      .groupByKey().map(x => {
      val user_id = x._1
      val basket_money_array = x._2.toArray
      user_id + "|" + StatsUtil.sta_Count(basket_money_array)
    })
    user_basket_money_sta
  }

  // 每单每个商品花了多少钱  (/basket_size)
  def get_user_log_money_sta(user_basket_money: RDD[(String, String, String, String, String)]):
      RDD[String] = {
    val user_log_money_sta = user_basket_money.map {
      case (user_id, uid, sldat, basket_money, basket_size) =>
        (user_id, basket_money.toDouble / basket_size.toDouble)
    }.groupByKey().map(x => {
      val user_id = x._1
      val avg_basket_money_array = x._2.toArray
      user_id + "|" + StatsUtil.sta_Count(avg_basket_money_array)
    })
    user_log_money_sta
  }

  def get_user_frequency(user_basket_money: RDD[(String, String, String, String, String)],
                         now_date: String): RDD[(String, (String, String, String, String))] = {
    val user_frequency = user_basket_money.map {
      case (user_id, uid, sldat, basket_money, basket_size) =>
        (user_id, dateTimeDiffInDay(sldat, now_date).toInt)
    }
      .filter(x => x._2 <= Constants.YEAR_CHOOSE)
      .groupByKey().map(x => {
      val user_id = x._1
      val sldat_Array = x._2.toList
      val year_month_day_count = FrequencyUtil.frequecy_count(Constants.ONE_MONTH, sldat_Array)
      val frequency_year = year_month_day_count._1.mkString("|")
      val frequency_month = year_month_day_count._2.mkString("|")
      val frequency_day = year_month_day_count._3
      val days_sta = StatsUtil.sta_Count(frequency_day.toArray)
      (user_id, (frequency_year, frequency_month, frequency_day.mkString("|"), days_sta))
    })
    user_frequency
  }

  def get_user_frequency_year(user_frequency: RDD[(String, (String, String, String, String))]):
      RDD[String] = {
    user_frequency.map {
      case (user_id, (frequency_year, frequency_month, frequency_day, days_sta)) =>
        user_id + "|" + frequency_year
    }
  }

  def get_user_frequency_month(user_frequency: RDD[(String, (String, String, String, String))]):
      RDD[String] = {
    user_frequency.map {
      case (user_id, (frequency_year, frequency_month, frequency_day, days_sta)) =>
        user_id + "|" + frequency_month
    }
  }

  def get_user_frequency_day(user_frequency: RDD[(String, (String, String, String, String))]):
      RDD[String] = {
    user_frequency.map {
      case (user_id, (frequency_year, frequency_month, frequency_day, days_sta)) =>
        user_id + "|" + frequency_day + "|" + days_sta
    }
  }

  def get_user_money(user_basket_money: RDD[(String, String, String, String, String)],
                     now_date: String): RDD[(String, (String, String, ListBuffer[Double], String))] = {
    val user_money = user_basket_money.map {
      case (user_id, uid, sldat, basket_money, basket_size) =>
        (user_id, Tuple2(dateTimeDiffInDay(sldat, now_date).toInt, basket_money.toDouble))
    }
      .filter(x => x._2._1 <= Constants.YEAR_CHOOSE)
      .groupByKey().map(x => {
        val user_id = x._1
        val days_money_Array = x._2.toList
        val year_month_day_money = FrequencyUtil.num_days_total_money(Constants.ONE_MONTH, days_money_Array)
        val money_year = year_month_day_money._1.mkString("|")
        val money_month = year_month_day_money._2.mkString("|")
        val money_day = year_month_day_money._3
        val sta_day = StatsUtil.sta_Count(money_day.toArray)
        (user_id, (money_year, money_month, money_day, sta_day))
      })
    user_money
  }

  def get_user_money_year(user_money: RDD[(String, (String, String, ListBuffer[Double], String))]):
      RDD[String] = {
    user_money.map { case (user_id, (money_year, money_month, money_day, sta_day)) =>
      user_id + "|" + money_year
    }
  }

  def get_user_money_month(user_money: RDD[(String, (String, String, ListBuffer[Double], String))]):
      RDD[String] = {
    user_money.map {
      case (user_id, (money_year, money_month, money_day, sta_day)) => user_id + "|" + money_month
    }

  }

  def get_user_money_day(user_money: RDD[(String, (String, String, ListBuffer[Double], String))]):
      RDD[String] = {
    val user_money_day = user_money.map {
      case (user_id, (money_year, money_month, money_day, sta_day)) => (user_id, (money_day, sta_day))
    }.filter(x => {
      val distinct_list = x._2._1.distinct
      //去掉每一天消费都是0的用户
      distinct_list.length != 1 || (distinct_list.length == 1 && distinct_list(0) != 0)
    })
      .map {
        case (user_id, (money_day, sta_day)) => user_id + "|" + money_day.mkString("|") + "|" + sta_day
      }
    user_money_day
  }

  def get_user_item_between_days(data_simplified: RDD[(String, String, String,
    String, String, String, String)], now_date: String): RDD[((String, String), Int)] = {
    data_simplified.map {
      case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        ((user_id, item_id), dateTimeDiffInDay(sldat, now_date).toInt)
    }
  }

  def get_user_item_frequency_month(user_item_between_days: RDD[((String, String), Int)]): RDD[String] = {
    val user_item_frequency_month = user_item_between_days.filter(x => x._2 <= Constants.MONTH_CHOOSE)
      .groupByKey().map(x => {
      val user_id = x._1._1
      val item_id = x._1._2
      val month_array = x._2.toList
      val count_month = FrequencyUtil.frequecy_month_count(month_array)
      user_id + "|" + item_id + "|" + count_month.mkString("|")
    })
    user_item_frequency_month
  }

  def get_user_item_frequency_day(user_item_between_days: RDD[((String, String), Int)]): RDD[String] = {
    val user_item_frequency_day = user_item_between_days.filter(x => x._2 <= Constants.USER_ITEM_DAYS)
      .groupByKey()
      .map(x => {
        val user_id = x._1._1
        val item_id = x._1._2
        val days_Array = x._2.toList
        val user_item_1_to_15_total = FrequencyUtil.num_days_total(Constants.USER_ITEM_DAYS, days_Array)
        (user_id, item_id, user_item_1_to_15_total)
      }).filter(x => {
      val distinct_list = x._3.distinct
      //过滤掉购物次数为0的用户
      distinct_list.length != 1 || (distinct_list.length == 1 && distinct_list(0) != 0)
    }).map {
      case ((user_id, item_id, user_item_1_to_15_total)) =>
        user_id + "|" + item_id + "|" + user_item_1_to_15_total.mkString("|")
    }
    user_item_frequency_day
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_4") {
      opt[String]("data_simplified_input_path")
        .action((x, c) => c.copy(data_simplified_input_path = x))
      opt[String]("user_basket_money_input_path")
        .action((x, c) => c.copy(user_basket_money_input_path = x))
      opt[Boolean]("save")
        .action((x, c) => c.copy(is_save = x))
      opt[String]("now_date")
        .action((x, c) => c.copy(now_date = x))
      opt[String]("item_price_sta_output_path")
        .action((x, c) => c.copy(item_price_sta_output_path = x))
      opt[String]("item_frequency_year_output_path")
        .action((x, c) => c.copy(item_frequency_year_output_path = x))
      opt[String]("item_frequency_month_output_path")
        .action((x, c) => c.copy(item_frequency_month_output_path = x))
      opt[String]("item_frequency_day_output_path")
        .action((x, c) => c.copy(item_frequency_day_output_path = x))
      opt[String]("user_basket_money_sta_output_path")
        .action((x, c) => c.copy(user_basket_money_sta_output_path = x))
      opt[String]("user_log_money_sta_output_path")
        .action((x, c) => c.copy(user_log_money_sta_output_path = x))
      opt[String]("user_frequency_year_output_path")
        .action((x, c) => c.copy(user_frequency_year_output_path = x))
      opt[String]("user_frequency_month_output_path")
        .action((x, c) => c.copy(user_frequency_month_output_path = x))
      opt[String]("user_frequency_day_output_path")
        .action((x, c) => c.copy(user_frequency_day_output_path = x))
      opt[String]("user_money_year_output_path")
        .action((x, c) => c.copy(user_money_year_output_path = x))
      opt[String]("user_money_month_output_path")
        .action((x, c) => c.copy(user_money_month_output_path = x))
      opt[String]("user_money_day_output_path")
        .action((x, c) => c.copy(user_money_day_output_path = x))
      opt[String]("user_item_frequency_month_output_path")
        .action((x, c) => c.copy(user_item_frequency_month_output_path = x))
      opt[String]("user_item_frequency_day_output_path")
        .action((x, c) => c.copy(user_item_frequency_day_output_path = x))
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

  case class Params(
                     data_simplified_input_path: String = "data/step_3/data_simplified",
                     user_basket_money_input_path: String = "data/step_3/user_basket_money",
                     is_save: Boolean = false,
                     now_date: String = "2018-03-08 00:00:00",
                     item_price_sta_output_path: String = "data/step_4/item_price_sta",
                     item_frequency_year_output_path: String = "data/step_4/item_frequency_year",
                     item_frequency_month_output_path: String = "data/step_4/item_frequency_month",
                     item_frequency_day_output_path: String = "data/step_4/item_frequency_day",
                     user_basket_money_sta_output_path: String = "data/step_4/user_basket_money_sta",
                     user_log_money_sta_output_path: String = "data/step_4/user_log_money_sta",
                     user_frequency_year_output_path: String = "data/step_4/user_frequency_year",
                     user_frequency_month_output_path: String = "data/step_4/user_frequency_month",
                     user_frequency_day_output_path: String = "data/step_4/user_frequency_day",
                     user_money_year_output_path: String = "data/step_4/user_money_year",
                     user_money_month_output_path: String = "data/step_4/user_money_month",
                     user_money_day_output_path: String = "data/step_4/user_money_day",
                     user_item_frequency_month_output_path: String = "data/step_4/user_item_frequency_month",
                     user_item_frequency_day_output_path: String = "data/step_4/user_item_frequency_day"
                   )

}
