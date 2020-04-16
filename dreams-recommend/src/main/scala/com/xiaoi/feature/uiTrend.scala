package com.xiaoi.feature

import com.xiaoi.common.HadoopOpsUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer
/*
 * step_5_6to7    购买趋势值   ui strike特征 
 */

object uiTrend {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("step_5_6 ui_strike")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext


    HadoopOpsUtil.removeDir(params.ui_order_strike_output_path, params.ui_order_strike_output_path)
    HadoopOpsUtil.removeDir(params.ui_strike_output_path, params.ui_strike_output_path)
    HadoopOpsUtil.removeDir(params.ui_trend_output_path, params.ui_trend_output_path)

    //user_id，item_id，basket_list
    // 准备数据  user_basket_has_item_list
    val user_baskets_has_item_list = get_baskets_items(
      params.user_baskets_has_item_list_intput_path, sc)

    logger.info("ui_order_strike write...")
    //step_5_6
    /**
     * user_id
     * item_id
     * strike_value
     */
    val ui_order_strike = get_ui_order_strike(user_baskets_has_item_list)
    ui_order_strike.saveAsTextFile(params.ui_order_strike_output_path)

    logger.info("ui_strike write...")
    // step_5_6
    /**
     * user_id
     * item_id
     * 、、、
     */
    val ui_strike = get_ui_strike(user_baskets_has_item_list)
    ui_strike.saveAsTextFile(params.ui_strike_output_path)

    logger.info("ui_trend data extracting...")
    //step_5_7
    /**
     * user_id
     * item_id
     * trend
     */
    val ui_trend = get_ui_trend(params.user_item_frequency_month_input_path,
      params.trend_delta, sc)
    ui_trend.saveAsTextFile(params.ui_trend_output_path)

    sc.stop()
  }

  def get_baskets_items(user_baskets_has_item_list_intput_path: String,
                        sc: SparkContext) = {
    val user_baskets_has_item_list = sc.textFile(user_baskets_has_item_list_intput_path)
      .map(line => {
        val strArr = line.split("\\|")
        val user_id = strArr(0)
        val item_id = strArr(1)
        val basket_list = ListBuffer[Int]()
        for (i <- 2 until strArr.length) {
          basket_list.append(strArr(i).toInt)
        }
        (user_id, item_id, basket_list)
      })
    user_baskets_has_item_list
  }

  def get_ui_order_strike(user_baskets_has_item_list:
                          RDD[(String, String, ListBuffer[Int])]) = {
    val ui_order_strike = user_baskets_has_item_list.map(x => {
      val user_id = x._1
      val item_id = x._2
      val basket_list = x._3
      //做一个倒序排列，因为要去最后五次的
      val basket_list_reverse = basket_list.reverse
      val len = basket_list_reverse.length
      //做一个判断，防止有些用户购买次数小于5次
      if (len < 5) {
        for (i <- len until 5) {
          basket_list_reverse.append(0)
        }
      }
      val five_basket = ListBuffer[Int]()
      for (i <- 0 until 5) {
        five_basket.append(basket_list_reverse(i))
      }
      val strike_value = count_pow(five_basket)
      user_id + "|" + item_id + "|" + strike_value
    })
    ui_order_strike
  }

  def get_ui_strike(user_baskets_has_item_list: RDD[(String, String, ListBuffer[Int])]) = {
    val ui_strike = user_baskets_has_item_list.map(x => {
      val user_id = x._1
      val item_id = x._2
      val basket_list = x._3

      //‘1to1', '11to1', '10to1', '111to1', '110to1', '101to1', '100to1'  count,
      // chance, ratio  => max，min，average, median
      def strike_concat(pattern: String, pattern_c: String, num_buy: Int): String = {
        val oneToOne = basket_list.sliding(num_buy).toList.map(_.mkString("|"))
        val oneToOneCount = oneToOne.filter(_ == pattern).size.toFloat
        val oneToOneChance = oneToOne.filter(x => x == pattern || x == pattern_c).size.toFloat
        val oneToOneRatio = if (oneToOneChance == 0) 0 else oneToOneCount / oneToOneChance
        List(oneToOneCount, oneToOneChance, oneToOneRatio).mkString("|")
      }

      val oo = "1,1"
      val oo_c = "1,0"

      val ooo = "1,1,1"
      val ooo_c = "1,1,0"

      val ono = "1,0,1"
      val ono_c = "1,0,0"

      val oooo = "1,1,1,1"
      val oooo_c = "1,1,1,0"

      val oono = "1,1,0,1"
      val oono_c = "1,1,0,0"

      val onoo = "1,0,1,1"
      val onoo_c = "1,0,1,0"

      val onno = "1,0,0,1"
      val onno_c = "1,0,0,0"
      user_id + "|" + item_id + "|" + List(strike_concat(oo, oo_c, 2),
        strike_concat(ooo, ooo_c, 3), strike_concat(ono, ono_c, 3),
        strike_concat(oooo, oooo_c, 4), strike_concat(oono, oono_c, 4),
        strike_concat(onoo, onoo_c, 4), strike_concat(onno, onno_c, 4)
      ).mkString("|")
    })
    ui_strike
  }

  def get_ui_trend(user_item_frequency_month_input_path: String,
                   trend_delta: Double,
                   sc: SparkContext) = {
    val ui_trend = sc.textFile(user_item_frequency_month_input_path)
      .map(line => {
        val strArr = line.split("\\|")
        val user_id = strArr(0)
        val item_id = strArr(1)
        val month_1 = strArr(2).toDouble
        val month_2 = strArr(3).toDouble
        val month_3 = strArr(4).toDouble
        val month_4 = strArr(5).toDouble
        val month_5 = strArr(6).toDouble
        val month_6 = strArr(7).toDouble
        val m12 = month_1 + month_2
        val m34 = month_3 + month_4
        val m56 = month_5 + month_6
        val trend_num = trend(trend_delta, m12, m34, m56)
        user_id + "|" + item_id + "|" + trend_num
      })
    ui_trend
  }

  //  三个值：1, 0, -1 分别表示：上升，平稳，下降
  def trend(trend_delta: Double, m12: Double, m34: Double, m56: Double) = {
    var trend_num = 0
    if ((m12 > m34 && m34 > m56) || (m12 + m34) > (m34 + m56) * (1 + trend_delta)) {
      trend_num = 1
    } else if ((m12 < m34 && m34 < m56) || (m12 + m34) < (m34 + m56) * (1 - trend_delta)) {
      trend_num = -1
    }
    trend_num
  }

  def count_pow(five_basket: ListBuffer[Int]): Double = {
    var result = 0.0
    for (i <- 0 until five_basket.length) {
      if (five_basket(i) == 1) {
        result += Math.pow(0.5, i + 1)
      }
    }
    result
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_3") {
      opt[String]("user_baskets_has_item_list_intput_path")
        .action((x, c) => c.copy(user_baskets_has_item_list_intput_path = x))
      opt[String]("user_item_frequency_month_input_path")
        .action((x, c) => c.copy(user_item_frequency_month_input_path = x))
      opt[Double]("trend_delta")
        .action((x, c) => c.copy(trend_delta = x))
      opt[String]("ui_order_strike_output_path")
        .action((x, c) => c.copy(ui_order_strike_output_path = x))
      opt[String]("ui_strike_output_path")
        .action((x, c) => c.copy(ui_strike_output_path = x))
      opt[String]("ui_trend_output_path")
        .action((x, c) => c.copy(ui_trend_output_path = x))
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
                     user_baskets_has_item_list_intput_path: String = "data/step_3/user_baskets_has_item_list",
                     user_item_frequency_month_input_path: String = "data/step_4/user_item_frequency_month",
                     is_save: Boolean = false,
                     trend_delta: Double = 0.1,
                     ui_order_strike_output_path: String = "data/step_5/ui_order_strike",
                     ui_strike_output_path: String = "data/step_5/ui_strike",
                     ui_trend_output_path: String = "data/step_5/ui_trend"
                   )

}
