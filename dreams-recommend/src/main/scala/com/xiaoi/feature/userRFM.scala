package com.xiaoi.feature

import com.xiaoi.common.{DateUtil, HadoopOpsUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/*
 *  step_6_12to31   实际还未用上   各种rfm模型   7个slots； 各种rfm模型的变化
 */

object userRFM {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val ONE_MONTH_DAYS = 30
  val RECENCY_DAYS = List(7, 14, 21, 30, 60, 90)
  val FREQUENCY = List(30, 20, 10, 6, 3, 2)
  val TOTAL_MONEY = List(2000, 1000, 500, 200, 100, 50)
  val LOSS_MODEL_PARAM = ListBuffer[Double](1, 2, 4)

  def run(params: Params) = {
    val conf = new SparkConf().setAppName("step_6_12to31 or userRFM").setMaster("local[*]")
    val sc = new SparkContext(conf)

    logger.info("read input file.....")
    val user_basket_money_info = get_user_basket_money_info(sc, params)
    val user_lifetime = get_user_lifetime(sc, params.user_lifetime_input_path)
    val user_sales = get_user_sales(sc, params.user_sales_input_path)
    val avg_user_lifetime = get_avg_user_lifetime(sc, params.avg_user_lifetime_input_path)
    val avg_user_sales = get_avg_user_sales(sc, params.avg_user_sales_input_path)

    val user_basket_money = user_basket_money_info._1
    val observe_time = user_basket_money_info._2

    val user_rfm = get_user_rfm(user_basket_money)

    val user_rfm_7slots = get_user_rfm_7slots(user_rfm, observe_time)

    logger.info("user_rfm_7slots  saving.....")
    HadoopOpsUtil.removeDir(params.user_rfm_7slots_output_path, params.user_rfm_7slots_output_path)
    user_rfm_7slots.saveAsTextFile(params.user_rfm_7slots_output_path)

    /*
    * 以下是还不确定的内容
    */
    val step_6_13 = get_step_6_13(user_lifetime)
    val step_6_21 = get_step_6_21(user_lifetime, user_sales, avg_user_lifetime, avg_user_sales)
    val step_6_22 = get_step_6_22(user_sales, avg_user_sales)
    val step_6_31 = get_step_6_31(user_lifetime, user_sales, avg_user_lifetime, avg_user_sales)

    sc.stop()
  }

  def get_user_basket_money_info(sc: SparkContext, params: Params) = {
    val user_basket_money = sc.textFile(params.user_basket_money_input_path)
      .map(line => {
        val strArr = line.split("\\|", -1)
        val user_id = strArr(0)
        val uid = strArr(1)
        val sldat = strArr(2)
        val basket_money = strArr(3)
        val basket_size = strArr(4)
        (user_id, uid, sldat, basket_money, basket_size)
      })

    val observe_day = if (params.observe_time_enabled) params.observe_time_input
    else {
      user_basket_money.map(_._3)
        .max()
        .slice(0, 10)
    }
    val observe_time = DateUtil.getTimeBefore(observe_day + " 00:00:00", -1)

    val user_basket_money_info = user_basket_money.filter(x => {
      if (params.limit_month_enabled) {
        val sldat = x._3
        val limit_days = params.limit_month * ONE_MONTH_DAYS
        DateUtil.dateTimeDiffInDay(sldat, observe_time) <= limit_days
      } else {
        true
      }
    })
    (user_basket_money_info, observe_time)
  }

  /*
  * 计算每个用户的R F M 值
  */
  def get_user_rfm(user_basket_money: RDD[(String, String, String, String, String)]) = {
    val user_rfm = user_basket_money.map {
      case (user_id, uid, sldat, basket_money, basket_size) =>
        (user_id, (uid, sldat, basket_money.toDouble, basket_size))
    }.groupByKey()
      .map(x => {
        val user_id = x._1
        val last_order_date = x._2.map(_._2).max
        val total_sessions = x._2.size
        val total_money = x._2.map(_._3).sum
        (user_id, last_order_date, total_sessions, total_money)
      })
    user_rfm
  }

  /*
  * 对7中情况分别赋值 1 or 0
  */
  def rfm_judge(rfm: List[Int], count: Int) = {
    val rfm_list = ListBuffer[Int]()
    for (i <- 0 until rfm.length) {
      if (i < count) {
        rfm_list.append(1)
      } else {
        rfm_list.append(0)
      }
    }
    rfm_list.reverse
  }

  /*
  * 分别对RFM 7 slots统计，符合条件为1，反之为0
  */
  def get_user_rfm_7slots(user_rfm: RDD[(String, String, Int, Double)],
                          observe_time: String) = {
    user_rfm.map(x => {
      val user_id = x._1
      val last_order_date = x._2
      val total_sessions = x._3
      val total_money = x._4

      val days = DateUtil.dateTimeDiffInDay(last_order_date, observe_time)
      val r_count = RECENCY_DAYS.filter(days <= _).size
      val r_list = rfm_judge(RECENCY_DAYS, r_count)

      val f_count = FREQUENCY.filter(total_sessions >= _).size
      val f_list = rfm_judge(FREQUENCY, f_count)

      val m_count = TOTAL_MONEY.filter(total_money >= _).size
      val m_list = rfm_judge(TOTAL_MONEY, m_count)
      List(user_id, r_list.mkString("|"), f_list.mkString("|"), m_list.mkString("|")).mkString("|")
    })
  }

  def get_user_lifetime(sc: SparkContext, user_lifetime_input_path: String) = {
    sc.textFile(user_lifetime_input_path)
      .map(line => {
        //user_id, first_order_date, last_order_date, life_days,
        // days_since_prior_order, days_between_orders
        val strArr = line.split("\\|")
        val user_id = strArr(0)
        val first_order_date = strArr(1)
        val last_order_date = strArr(2)
        val life_days = strArr(3)
        val days_since_prior_order = strArr(4)
        val days_between_orders = strArr(5)
        (user_id, first_order_date, last_order_date, life_days,
          days_since_prior_order, days_between_orders)
      })
  }

  def get_user_sales(sc: SparkContext, user_sales_input_path: String) = {
    sc.textFile(user_sales_input_path)
      .map(line => {
        //user_id, total_logs, total_sessions, total_money,
        //sessions_per_year, money_per_year, money_per_session, logs_per_session
        val strArr = line.split("\\|")
        val user_id = strArr(0)
        val total_logs = strArr(1)
        val total_sessions = strArr(2)
        val total_money = strArr(3)
        val sessions_per_year = strArr(4)
        val money_per_year = strArr(5)
        val money_per_session = strArr(6)
        val logs_per_session = strArr(7)
        (user_id, total_logs, total_sessions, total_money, sessions_per_year,
          money_per_year, money_per_session, logs_per_session)
      })
  }

  def get_avg_user_lifetime(sc: SparkContext,
                            avg_user_lifetime_input_path: String) = {
    sc.textFile(avg_user_lifetime_input_path)
      .map(line => {
        //avg_life_days, avg_days_since_prior_order, avg_days_between_orders
        val strArr = line.split("\\|")
        val avg_life_days = strArr(0)
        val avg_days_since_prior_order = strArr(1)
        val avg_days_between_orders = strArr(2)
        (avg_life_days, avg_days_since_prior_order, avg_days_between_orders)
      })
  }

  def get_avg_user_sales(sc: SparkContext, avg_user_sales_input_path: String) = {
    sc.textFile(avg_user_sales_input_path)
      .map(line => {
        //avg_logs, avg_sessions, avg_money, avg_sessions_per_year,
        //avg_money_per_year, avg_money_per_session, avg_logs_per_session
        val strArr = line.split("\\|")
        val avg_logs = strArr(0)
        val avg_sessions = strArr(1)
        val avg_money = strArr(2)
        val avg_sessions_per_year = strArr(3)
        val avg_money_per_year = strArr(4)
        val avg_money_per_session = strArr(5)
        val avg_logs_per_session = strArr(6)
        (avg_logs, avg_sessions, avg_money, avg_sessions_per_year,
          avg_money_per_year, avg_money_per_session, avg_logs_per_session)
      })
  }

  // 简单的客户流失模型 calculate R/T  (T is days between orders)
  def get_step_6_13(user_lifetime: RDD[(String, String, String, String, String, String)]) = {
    user_lifetime.map {
      case (user_id, first_order_date, last_order_date, life_days,
      days_since_prior_order, days_between_orders) =>
        (user_id, days_since_prior_order.toDouble / days_between_orders.toDouble)
    }.map(x => {
      val user_id = x._1
      val range = x._2

      val loss_model = LOSS_MODEL_PARAM
        loss_model.append(range)
      val sort_list = loss_model.sortWith(_ < _)
      //根据条件确定长度
      val result_list = ListBuffer[Int]()
      for (i <- 0 until loss_model.length) {
        result_list.append(0)
      }
      result_list(sort_list.indexOf(range)) = 1

      (user_id, result_list)
    })
  }

  // RFM相关特征选取：   从最近访问时间， 访问频率， 访问金额  这三个方面选取相关特征。
  def get_step_6_21(user_lifetime: RDD[(String, String, String, String, String, String)],
                    user_sales: RDD[(String, String, String, String, String, String, String, String)],
                    avg_user_lifetime: RDD[(String, String, String)],
                    avg_user_sales: RDD[(String, String, String, String, String, String, String)]) = {
    val user_lifetime_info = user_lifetime.map {
      case (user_id, first_order_date, last_order_date, life_days,
        days_since_prior_order, days_between_orders) => 
        (user_id, days_since_prior_order)
    }

    val user_sales_info = user_sales.map {
      case (user_id, total_logs, total_sessions, total_money, sessions_per_year,
        money_per_year, money_per_session, logs_per_session) =>
        (user_id, (total_sessions, total_money))
    }

    val avg_days_since_prior_order = avg_user_lifetime.collect()(0)._2.toDouble
    val avg_sessions = avg_user_sales.collect()(0)._2.toDouble
    val avg_money = avg_user_sales.collect()(0)._3.toDouble

    user_lifetime_info.join(user_sales_info)
      .map(x => {
        val user_id = x._1
        val days_since_prior_order = x._2._1.toDouble
        val total_sessions = x._2._2._1.toDouble
        val total_money = x._2._2._2.toDouble

        val recency_value = if (days_since_prior_order <= avg_days_since_prior_order) 1 else 0
        val frequency_value = if (total_sessions >= avg_sessions) 1 else 0
        val money_value = if (total_money >= avg_money) 1 else 0
        (user_id, recency_value, frequency_value, money_value)
      })
  }

  // 以平均金额、 购买次数来衡量， 分为   最好的客户、 乐于消费性客户、经常性客户、不确定型客户
  def get_step_6_22(user_sales: RDD[(String, String, String, String, String, String, String, String)],
                    avg_user_sales: RDD[(String, String, String, String, String, String, String)]) = {
    val user_sales_info = user_sales.map {
      case (user_id, total_logs, total_sessions, total_money, sessions_per_year,
      money_per_year, money_per_session, logs_per_session) =>
        (user_id, sessions_per_year, money_per_year)
    }

    //avg_logs, avg_sessions, avg_money, avg_sessions_per_year,
    //avg_money_per_year, avg_money_per_session, avg_logs_per_session
    val avg_sessions_per_year = avg_user_sales.collect()(0)._4.toDouble
    val avg_money_per_year = avg_user_sales.collect()(0)._5.toDouble

    user_sales_info.map(x => {
      val user_id = x._1
      val sessions_per_year = x._2.toDouble
      val money_per_year = x._3.toDouble

      val f_value = if (sessions_per_year >= avg_sessions_per_year) 1 else 0
      val m_value = if (money_per_year >= avg_money_per_year) 1 else 0
      (user_id, f_value, m_value)
    })

  }

  // 客户忠诚度模型  CLV (customer loyalty value) = T*M*P
  def get_step_6_31(user_lifetime: RDD[(String, String, String, String, String, String)],
                    user_sales: RDD[(String, String, String, String, String, String, String, String)],
                    avg_user_lifetime: RDD[(String, String, String)],
                    avg_user_sales: RDD[(String, String, String, String, String, String, String)]) = {

    val user_sales_info = user_sales.map {
      case (user_id, total_logs, total_sessions, total_money, sessions_per_year,
      money_per_year, money_per_session, logs_per_session) =>
        (user_id, sessions_per_year, total_money, logs_per_session)
    }
    //avg_logs, avg_sessions, avg_money, avg_sessions_per_year,
    //avg_money_per_year, avg_money_per_session, avg_logs_per_session
    val avg_sessions_per_year = avg_user_sales.collect()(0)._4.toDouble
    val avg_money = avg_user_sales.collect()(0)._3.toDouble

    user_sales_info.map(x => {
      val user_id = x._1
      val sessions_per_year = x._2.toDouble
      val total_money = x._3.toDouble
      val logs_per_session = x._4.toDouble
      val clv = sessions_per_year / avg_sessions_per_year +
        total_money / avg_money +
        Math.log10(logs_per_session)
      (user_id, clv)
    })
  }


  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_6_12to31") {
      opt[String]("user_basket_money_input_path")
        .action((x, c) => c.copy(user_basket_money_input_path = x))
      opt[Boolean]("limit_month_enabled")
        .action((x, c) => c.copy(limit_month_enabled = x))
      opt[Int]("limit_month")
        .action((x, c) => c.copy(limit_month = x))
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
                     user_basket_money_input_path: String = "data/step_3/user_basket_money",
                     user_lifetime_input_path: String = "data/step_6/user_lifetime",
                     user_sales_input_path: String = "data/step_6/user_sales",
                     avg_user_lifetime_input_path: String = "data/step_6/avg_user_lifetime",
                     avg_user_sales_input_path: String = "data/step_6/avg_user_sales",
                     limit_month_enabled: Boolean = true,
                     limit_month: Int = 6,
                     observe_time_enabled: Boolean = false,
                     observe_time_input: String = "2018-03-07",
                     user_rfm_7slots_output_path: String = "data/step_6/user_rfm_7slots"
                   )

}
