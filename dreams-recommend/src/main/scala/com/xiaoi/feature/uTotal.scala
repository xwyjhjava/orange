package com.xiaoi.feature

import com.xiaoi.common.{DateUtil, HadoopOpsUtil, StatsUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.{ListBuffer, _}

object uTotal {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("step_5_21to23 or uTotal")
    val sc = new SparkContext(conf)
    HadoopOpsUtil.removeDir(params.u_each_item_count_sta_output_path, params.u_each_item_count_sta_output_path)
    HadoopOpsUtil.removeDir(params.u_total_sessions_rank_percent_output_path, params.u_total_sessions_rank_percent_output_path)
    HadoopOpsUtil.removeDir(params.u_meta_info_output_path, params.u_meta_info_output_path)
    HadoopOpsUtil.removeDir(params.u_items_top5_output_path, params.u_items_top5_output_path)
    HadoopOpsUtil.removeDir(params.u_item_price_sta_output_path, params.u_item_price_sta_output_path)
    HadoopOpsUtil.removeDir(params.u_total_money_rank_percent_output_path, params.u_total_money_rank_percent_output_path)
    HadoopOpsUtil.removeDir(params.u_basket_size_sta_output_path, params.u_basket_size_sta_output_path)
    HadoopOpsUtil.removeDir(params.u_basket_money_slot_percent_output_path, params.u_basket_money_slot_percent_output_path)

    val user_info_rdd = get_user_info(sc,
      params.data_simplified_input_path,
      params.user_total_input_path,
      params.user_detail_input_path,
      params.user_basket_money_input_path)
    val data_simplified = user_info_rdd._1
    val user_total = user_info_rdd._2
    val user_detail = user_info_rdd._3
    val user_basket_money = user_info_rdd._4
    val user_item_count = user_info_rdd._5

    logger.info("u_each_item_count_sta write...")
    val u_each_item_count_sta = get_each_item_count(user_item_count)
    u_each_item_count_sta.repartition(1) .saveAsTextFile(params.u_each_item_count_sta_output_path)

    logger.info("u_total_sessions_rank_percent write...")
    val u_total_sessions_rank_percent = get_u_total_session_rank(user_total)
    u_total_sessions_rank_percent.repartition(1) .saveAsTextFile(params.u_total_sessions_rank_percent_output_path)

    logger.info("u_meta_info write...")
    val u_meta_info = get_u_meta_info(user_detail)
    u_meta_info.repartition(1).saveAsTextFile(params.u_meta_info_output_path)

    logger.info("u_items_top5 write...")
    val u_items_top5 = get_u_items_top5(user_item_count,
      user_total, params.topn_user, params.topn_item)
    u_items_top5.repartition(1).saveAsTextFile(params.u_items_top5_output_path)

    logger.info("u_item_price_sta write...")
    val u_item_price_sta = get_u_item_price(data_simplified)
    u_item_price_sta.repartition(1).saveAsTextFile(params.u_item_price_sta_output_path)

    logger.info("u_total_money_rank_percent write...")
    val u_total_money_rank_percent = get_u_total_money_rank(sc,
      user_total, params.user_money_year_input_path)
    u_total_money_rank_percent.repartition(1)
      .saveAsTextFile(params.u_total_money_rank_percent_output_path)

    logger.info("u_basket_size_sta write...")
    val u_basket_size_sta = get_u_basket_size(user_basket_money)
    u_basket_size_sta.repartition(1)
      .saveAsTextFile(params.u_basket_size_sta_output_path)

    logger.info("u_basket_money_slot_percent write...")
    val u_basket_money_slot_percent = get_u_basket_slot(user_basket_money)
    u_basket_money_slot_percent.repartition(1)
      .saveAsTextFile(params.u_basket_money_slot_percent_output_path)

    val run = Runtime.getRuntime
    println(s"The maxMemory: ${run.maxMemory() / math.pow(1024, 2)}M")
    println(s"The freeMemory: ${run.freeMemory() / math.pow(1024, 2)}M")
    println(s"Used maxMemory ${
      (Runtime.getRuntime().totalMemory() -
        Runtime.getRuntime().freeMemory()) / math.pow(1024, 2)
    }M")

    sc.stop()
  }

  def get_user_info(sc: SparkContext,
                    data_simplified_input_path: String,
                    user_total_input_path: String,
                    user_detail_input_path: String,
                    user_basket_money_input_path: String) = {
    //uid, sldat, user_id，item_id，dptno, qty, amt
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
    //user_id, first_order_time, log_count, item_count, basket_count, money
    val user_total = sc.textFile(user_total_input_path).map(line => {
      val strArr = line.split("\\|", -1)
      val user_id = strArr(0)
      val first_order_time = strArr(1)
      val log_count = strArr(2)
      val item_count = strArr(3)
      val basket_count = strArr(4)
      val money = strArr(5)
      (user_id, first_order_time, log_count, item_count, basket_count, money)
    })
    //user_id，vipno, vip_gender, vip_birthday, vip_created_date
    val user_detail = sc.textFile(user_detail_input_path).map(line => {
      val strArr = line.split("\\|", -1)
      val user_id = strArr(0)
      val vipno = strArr(1)
      val vip_gender = strArr(2)
      val vip_birthday = strArr(3)
      val vip_created_date = strArr(4)
      (user_id, vipno, vip_gender, vip_birthday, vip_created_date)
    })
    //user_id, uid, sldat, basket_money, basket_size
    val user_basket_money = sc.textFile(user_basket_money_input_path).map(line => {
      val strArr = line.split("\\|", -1)
      val user_id = strArr(0)
      val uid = strArr(1)
      val sldat = strArr(2)
      val basket_money = strArr(3)
      val basket_size = strArr(4)
      (user_id, uid, sldat, basket_money, basket_size)
    })
    val user_item_count = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) => ((user_id, item_id), 1) }
      .reduceByKey(_ + _)
    (data_simplified, user_total, user_detail, user_basket_money, user_item_count)
  }

  def get_each_item_count(user_item_count: RDD[((String, String), Int)]) = {
    val u_each_item_count_sta = user_item_count
      .map { case ((user_id, item_id), ui_count) => (user_id, ui_count.toDouble) }
      .groupByKey()
      .map(x => {
        val user_id = x._1
        val ui_count_array = x._2.toArray
        val sta = StatsUtil.sta_Count(ui_count_array)
        user_id + "|" + sta
      })
    u_each_item_count_sta
  }

  // 该用户交易回合数在所有用户交易回合数排序中的百分比
  // 该用户有多么喜欢来购物
  def get_u_total_session_rank(user_total: RDD[(String, String, String, String, String, String)]) = {
    //用户的数量
    val user_count = user_total.count()
    val u_total_sessions_rank_percent = user_total
      .map { case (user_id, first_order_time, log_count, item_count, basket_count, money) =>
        (basket_count.toDouble, user_id)
      }
      .sortByKey(false)
      .zipWithIndex() //索引从0开始的
      .map { case ((basket_count, user_id), index) => user_id + "|" +
      (index + 1).toDouble / user_count }
    u_total_sessions_rank_percent
  }

  def get_u_meta_info(user_detail: RDD[(String, String, String, String, String)]) = {
    val u_meta_info = user_detail
      //生日为空get当前时间
      .map { case (user_id, vipno, vip_gender, vip_birthday, vip_created_date) =>
      val birthDay = if (vip_birthday.size < 10) DateUtil.getDate()
      else vip_birthday
      user_id + "|" +
        (DateUtil.getDate().split("-")(0).toInt - birthDay.split("-")(0).toInt)
    }
    u_meta_info
  }

  def get_u_items_top5(user_item_count: RDD[((String, String), Int)],
                       user_total: RDD[(String, String, String, String, String, String)],
                       topn_user: Int, topn_item: Int) = {
    val filter_topn_user_item = user_item_count
      .map { case ((user_id, item_id), ui_count) => (user_id, (ui_count, item_id)) }
      .groupByKey()
      .map(x => {
        val user_id = x._1
        //商品数量和商品id的集合
        val values = x._2.toArray
        //从大到小排序，并且给一个索引值（从0开始的）
        val itemd_count_sort_index = values
          .sortWith((x, y) => (x._1 > y._1)).zipWithIndex
        (user_id, itemd_count_sort_index)
      }).flatMap(x => {
      val user_id = x._1
      val itemd_count_sort_index = x._2
      itemd_count_sort_index
        .map { case ((ui_count, item_id), index) => (user_id, (item_id, ui_count, index + 1)) }
    }).filter(x => x._2._3 <= topn_item) //每个用户只取购买次数topN的数据

    val filter_topn_user = user_total
      .map { case (user_id, first_order_time, log_count, item_count, basket_count, money) =>
        (log_count.toDouble, user_id)
      }
      .sortByKey(false)
      .zipWithIndex()
      .map { case ((log_count, user_id), index) => (user_id, index + 1) }
      .filter(x => x._2 <= topn_user)

    val u_items_top5 = filter_topn_user_item.join(filter_topn_user)
      .map { case (user_id, ((item_id, ui_count, ui_index), user_index)) =>
        user_id + "|" + item_id + "|" + item_id + "|" + ui_count + "|" + ui_index
      }
    u_items_top5
  }

  // 所买商品价格最小值，最大值，中位数，平均值 , 标准差
  def get_u_item_price(data_simplified: RDD[(String, String, String, String, String, String, String)]) = {
    val u_item_price_sta = data_simplified.filter(x => x._6.toDouble != 0)
      .map {
        case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
          ((user_id, item_id), (qty.toDouble, amt.toDouble))
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map { case ((user_id, item_id), (qty_all, amt_all)) => (user_id, amt_all / qty_all) }
      .groupByKey()
      .map(x => {
        val user_id = x._1
        val item_avg_array = x._2.toArray
        val sta = StatsUtil.sta_Count(item_avg_array)
        user_id + "|" + sta
      })
    u_item_price_sta
  }

  // 本用户的总花费在所有用户花费金额排序中的百分比
  def get_u_total_money_rank(sc: SparkContext,
                             user_total: RDD[(String, String, String, String, String, String)],
                             user_money_year_input_path: String) = {
    //用户的数量
    val user_count = user_total.count()
    //user_id, half_year, 2nd_half_year, 2nd_year, 3rd_year
    val u_total_money_rank_percent = sc.textFile(user_money_year_input_path).map(line => {
      val strArr = line.split("\\|")
      val user_id = strArr(0)
      val total_money = strArr(1).toDouble + strArr(2).toDouble + strArr(3).toDouble +
        strArr(4).toDouble
      (total_money, user_id)
    }).sortByKey(false)
      .zipWithIndex()
      .map { case ((total_money, user_id), index) => user_id + "|" + total_money + "|" +
        (index + 1).toDouble / user_count }
    u_total_money_rank_percent
  }

  def get_u_basket_size(user_basket_money: RDD[(String, String, String, String, String)]) = {
    val u_basket_size_sta = user_basket_money
      .map {
        case (user_id, uid, sldat, basket_money, basket_size) => (user_id, basket_size.toDouble)
      }.groupByKey()
      .map(x => {
        val user_id = x._1
        val basket_size_array = x._2.toArray
        val sta = StatsUtil.sta_Count(basket_size_array)
        user_id + "|" + sta
      })
    u_basket_size_sta
  }

  // 每单金额金额区间占比
  def get_u_basket_slot(user_basket_money:
                        RDD[(String, String, String, String, String)]) = {
    val u_basket_money_slot_percent = user_basket_money
      .map {
        case (user_id, uid, sldat, basket_money, basket_size) => (user_id, basket_money.toDouble)
      }.groupByKey()
      .map(x => {
        val user_id = x._1
        val basket_money_array = x._2.toArray
        //用户在每个交易区间的占比
        val slot_list = money_slot(basket_money_array)
        user_id + "|" + slot_list.mkString("|")
      })
    u_basket_money_slot_percent
  }

  //  【0，50）、【50，200）、【200，500）、【500，1000）、【1000，+∞）
  def money_slot(arr: Array[Double]) = {
    val list = List[(Double, Double)]((0, 50), (50, 200), (200, 500), (500, 1000),
      (1000.0, Double.MaxValue))
    val slot_map = Map[Tuple2[Double, Double], Double]()
    for (i <- 0 until list.length) {
      slot_map.put(list(i), 0)
    }
    //交易回合数
    val basket_count = arr.length
    arr.map(basket_money => {
      list.map(x => {
        if (basket_money < x._2 && basket_money >= x._1) {
          slot_map.put(x, slot_map(x) + 1)
        }
      })
    })
    val slot_list = ListBuffer[Double]()
    for (i <- 0 until list.length) {
      slot_list.append(slot_map(list(i)) / basket_count)
    }
    slot_list
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_5_21") {
      opt[String]("data_simplified_input_path")
        .action((x, c) => c.copy(data_simplified_input_path = x))
      opt[String]("user_detail_input_path")
        .action((x, c) => c.copy(user_detail_input_path = x))
      opt[String]("user_total_input_path")
        .action((x, c) => c.copy(user_total_input_path = x))
      opt[String]("user_basket_money_input_path")
        .action((x, c) => c.copy(user_basket_money_input_path = x))
      opt[String]("user_money_year_input_path")
        .action((x, c) => c.copy(user_money_year_input_path = x))
      opt[String]("Pluname2id_input_path")
        .action((x, c) => c.copy(Pluname2id_input_path = x))
      opt[Int]("topn_item")
        .action((x, c) => c.copy(topn_item = x))
      opt[Int]("topn_user")
        .action((x, c) => c.copy(topn_user= x))
      opt[String]("u_each_item_count_sta_output_path")
        .action((x, c) => c.copy(u_each_item_count_sta_output_path = x))
      opt[String]("u_total_sessions_rank_percent_output_path")
        .action((x, c) => c.copy(u_total_sessions_rank_percent_output_path = x))
      opt[String]("u_meta_info_output_path")
        .action((x, c) => c.copy(u_meta_info_output_path = x))
      opt[String]("u_items_top5_output_path")
        .action((x, c) => c.copy(u_items_top5_output_path = x))
      opt[String]("u_item_price_sta_output_path")
        .action((x, c) => c.copy(u_item_price_sta_output_path = x))
      opt[String]("u_total_money_rank_percent_output_path")
        .action((x, c) => c.copy(u_total_money_rank_percent_output_path = x))
      opt[String]("u_basket_size_sta_output_path")
        .action((x, c) => c.copy(u_basket_size_sta_output_path = x))
      opt[String]("u_basket_money_slot_percent_output_path")
        .action((x, c) => c.copy(u_basket_money_slot_percent_output_path = x))
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
                     user_detail_input_path: String = "data/step_3/user_detail",
                     user_total_input_path: String = "data/step_3/user_total",
                     user_basket_money_input_path: String = "data/step_3/user_basket_money",
                     user_money_year_input_path: String = "data/step_4/user_money_year",
                     Pluname2id_input_path: String = "",
                     topn_item: Int = 5,
                     topn_user: Int = 10000,
                     u_each_item_count_sta_output_path: String = "data/step_5/u_each_item_count_sta",
                     u_total_sessions_rank_percent_output_path: String = "data/step_5/u_total_sessions_rank_percent",
                     u_meta_info_output_path: String = "data/step_5/u_meta_info",
                     u_items_top5_output_path: String = "data/step_5/u_items_top5",
                     u_item_price_sta_output_path: String = "data/step_5/u_item_price_sta",
                     u_total_money_rank_percent_output_path: String = "data/step_5/u_total_money_rank_percent",
                     u_basket_size_sta_output_path: String = "data/step_5/u_basket_size_sta",
                     u_basket_money_slot_percent_output_path: String = "data/step_5/u_basket_money_slot_percent"
                   )

}
