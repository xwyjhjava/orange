package com.xiaoi.feature

import java.text.SimpleDateFormat
import java.util.Date

import com.xiaoi.common.{DateUtil, HadoopOpsUtil, StrUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

/**
  * Written by siyuan.liu in 2018/8/8
  * step_6_11  user深度挖掘数据汇总
  */
object uDeep {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val YEAY_DAY = 365.24

  def run(params: Params) = {
    val conf = new SparkConf().setAppName("step_6_11 or uDeep")
    val sc = new SparkContext(conf)
    remove_dir(params)

    logger.info("读取输入文件.....")
    val data_simplified_input_path = params.data_simplified_input_path
    val start_date_enabled = params.start_date_enabled
    val start_date = params.start_date
    val observe_date_enabled = params.observe_date_enabled
    val observe_date = params.observe_date
    val data_simplified_info = get_data_simplified(sc, data_simplified_input_path,
          start_date_enabled, start_date, observe_date_enabled, observe_date)

    val data_simplified = data_simplified_info._1
    val observe_time = data_simplified_info._2

    val user_lifetime_sales = get_user_lifetime_sales(data_simplified, observe_time)

    val user_lifetime = get_user_lifetime(user_lifetime_sales)
    val user_sales = get_user_sales(user_lifetime_sales)

    logger.info("user_lifetime saving.....")
    user_lifetime.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.user_lifetime_output_path)

    logger.info("user_sales saving.....")
    user_sales.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.user_sales_output_path)

    logger.info("avg_user_lifetime saving.....")
    val avg_user_lifetime = get_avg_user_lifetime(sc, user_lifetime)
      .map(StrUtil.tupleToString(_, "|"))
    avg_user_lifetime.repartition(1)
      .saveAsTextFile(params.avg_user_lifetime_output_path)

    logger.info("avg_user_sales saving.....")
    val avg_user_sales = get_avg_user_sales(sc, user_sales).map(StrUtil.tupleToString(_, "|"))
    avg_user_sales.repartition(1).saveAsTextFile(params.avg_user_sales_output_path)

    sc.stop()

  }

  def remove_dir(params: Params) = {
    HadoopOpsUtil.removeDir(params.user_lifetime_output_path,
      params.user_lifetime_output_path)
    HadoopOpsUtil.removeDir(params.user_sales_output_path,
      params.user_sales_output_path)
    HadoopOpsUtil.removeDir(params.avg_user_lifetime_output_path,
      params.avg_user_lifetime_output_path)
    HadoopOpsUtil.removeDir(params.avg_user_sales_output_path,
      params.avg_user_sales_output_path)
  }

  def get_data_simplified(sc: SparkContext, data_simplified_input_path: String,
                          start_date_enabled: Boolean, start_date: String,
                          observe_date_enabled: Boolean, observe_date: String) = {
    val data_simplified = sc.textFile(data_simplified_input_path).map(line => {
      val strArr = line.split("\\|", -1)
      val uid = strArr(0)
      val sldat = strArr(1)
      val user_id = strArr(2)
      val item_id = strArr(3)
      val dptno = strArr(4)
      val qty = strArr(5)
      val amt = strArr(6)
      (uid, sldat, user_id, item_id, dptno, qty, amt)
    }).filter(x => {
      !x._3.equals("")
    })
      .filter(x => {
        if (start_date_enabled) {
          val sldat = x._2
          val start_time = start_date + " 00:00:00"
          DateUtil.getTimestamp(sldat) >= DateUtil.getTimestamp(start_time)
        } else {
          true
        }
      })

    val observe_day = if (observe_date_enabled) observe_date
      else {
        data_simplified.map(_._2)
          .max()
          .slice(0, 10)
      }
    val observe_time = DateUtil.getTimeBefore(observe_day + " 00:00:00", -1)
    (data_simplified, observe_time)
  }

  //通过时间戳获取字符串
  def getTimeStr(time: Long): String = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time))
  }

  /*
  * 计算所有的字段值
  */
  def get_user_lifetime_sales(data_simplified: RDD[(String, String, String, String, String, String, String)],
                              observe_time: String) = {
    data_simplified.map {
      case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        ((user_id, uid), (DateUtil.getTimestamp(sldat), amt.toDouble))
    }.groupByKey()
     .mapValues(x => {
        val sldat_time = x.map(_._1).max
        val busket_money = x.map(_._2).reduce(_ + _)
        val busket_logs = x.size
        (sldat_time, busket_money, busket_logs)
      }).map {
        case ((user_id, uid), (sldat_time, busket_money, busket_logs)) =>
          (user_id, (sldat_time, busket_money, busket_logs))
      }.groupByKey()
      .map(x => {
        val user_id = x._1
        val sldat_time_iterable = x._2.map(_._1)
        val busket_money_iterable = x._2.map(_._2)
        val busket_logs_iterable = x._2.map(_._3)
        val first_order_date = getTimeStr(sldat_time_iterable.min)
        val last_order_date = getTimeStr(sldat_time_iterable.max)
        val total_sessions = x._2.size
        val total_money = busket_money_iterable.reduce(_ + _)
        val total_logs = busket_logs_iterable.reduce(_ + _)

        val life_days_value = DateUtil.dateTimeDiffInDay(first_order_date, observe_time)
        val life_days = if (life_days_value == 0) 1 else life_days_value
        val days_since_prior_order = DateUtil.dateTimeDiffInDay(last_order_date, observe_time)
        val days_between_orders = life_days / total_sessions

        val sessions_per_year = total_sessions / (life_days / YEAY_DAY)
        val money_per_year = total_money / (life_days / YEAY_DAY)
        val money_per_session = total_money / total_sessions
        val logs_per_session = total_logs.toDouble / total_sessions

        (user_id, first_order_date, last_order_date, life_days, days_since_prior_order,
          days_between_orders, total_logs, total_sessions, total_money, sessions_per_year,
          money_per_year, money_per_session, logs_per_session)
      })
  }

  // 客龄： (观察点时间-first_order_date)    单位：天数; 客龄/total_sessions    平均两次购物的间隔时间
  def get_user_lifetime(user_lifetime_sales: RDD[(String, String, String, Long, Long, Long,
      Int, Int, Double, Double, Double, Double, Double)]) = {
    user_lifetime_sales.map {
      case (user_id, first_order_date, last_order_date, life_days, days_since_prior_order,
        days_between_orders, total_logs, total_sessions, total_money, sessions_per_year,
      money_per_year, money_per_session, logs_per_session) =>
        (user_id, first_order_date, last_order_date, life_days,
          days_since_prior_order, days_between_orders)
    }
  }

  // 年均购物次数; 年均金额; 单均金额;单均商品数
  def get_user_sales(user_lifetime_sales: RDD[(String, String, String, Long, Long, Long,
      Int, Int, Double, Double, Double, Double, Double)]) = {
    user_lifetime_sales.map {
      case (user_id, first_order_date, last_order_date, life_days, days_since_prior_order,
        days_between_orders, total_logs, total_sessions, total_money, sessions_per_year,
      money_per_year, money_per_session, logs_per_session) =>
        (user_id, total_logs, total_sessions, total_money, sessions_per_year,
          money_per_year, money_per_session, logs_per_session)
    }
  }

  def get_avg_user_lifetime(sc: SparkContext, 
       user_lifetime: RDD[(String, String, String, Long, Long, Long)]) = {
    val user_lifetime_info = user_lifetime.map {
      case (user_id, first_order_date, last_order_date, life_days,
      days_since_prior_order, days_between_orders) =>
        (life_days, days_since_prior_order, days_between_orders)
    }

    val count = user_lifetime_info.count().toDouble
    val total_user_lifetime = user_lifetime_info.reduce((a, b) =>
      (a._1 + b._1, a._2 + b._2, a._3 + b._3))
    val avg_user_lifetime_array = Array(total_user_lifetime)
      .map(x =>
        (x._1 / count, x._2 / count, x._3 / count)
      )
    sc.makeRDD(avg_user_lifetime_array)

  }

  def get_avg_user_sales(sc: SparkContext,
       user_sales: RDD[(String, Int, Int, Double, Double, Double, Double, Double)]) = {
    val user_sales_info = user_sales.map {
      case (user_id, total_logs, total_sessions, total_money, sessions_per_year,
        money_per_year, money_per_session, logs_per_session) =>
        (total_logs, total_sessions, total_money, sessions_per_year,
          money_per_year, money_per_session, logs_per_session)
    }

    val count = user_sales_info.count().toDouble
    val total_user_sales = user_sales_info.reduce((a, b) =>
      (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7))
    val avg_user_sales_array = Array(total_user_sales)
      .map(x =>
        (x._1 / count, x._2 / count, x._3 / count, x._4 / count, x._5 / count, x._6 / count, x._7 / count)
      )
    sc.makeRDD(avg_user_sales_array)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_6_11") {
      opt[String]("data_simplified_input_path")
        .action((x, c) => c.copy(data_simplified_input_path = x))
      opt[Boolean]("start_date_enabled")
        .action((x, c) => c.copy(start_date_enabled = x))
      opt[String]("start_date")
        .action((x, c) => c.copy(start_date = x))
      opt[Boolean]("observe_date_enabled")
        .action((x, c) => c.copy(observe_date_enabled = x))
      opt[String]("observe_date")
        .action((x, c) => c.copy(observe_date = x))
      opt[String]("user_lifetime_output_path")
        .action((x, c) => c.copy(user_lifetime_output_path = x))
      opt[String]("user_sales_output_path")
        .action((x, c) => c.copy(user_sales_output_path = x))
      opt[String]("avg_user_lifetime_output_path")
        .action((x, c) => c.copy(avg_user_lifetime_output_path = x))
      opt[String]("avg_user_sales_output_path")
        .action((x, c) => c.copy(avg_user_sales_output_path = x))
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
                     start_date_enabled: Boolean = true,
                     start_date: String = "2017-03-07",
                     observe_date_enabled: Boolean = false,
                     observe_date: String = "2018-03-07",
                     user_lifetime_output_path: String = "data/step_6/user_lifetime",
                     user_sales_output_path: String = "data/step_6/user_sales",
                     avg_user_lifetime_output_path: String = "data/step_6/avg_user_lifetime",
                     avg_user_sales_output_path: String = "data/step_6/avg_user_sales"
                   )

}
