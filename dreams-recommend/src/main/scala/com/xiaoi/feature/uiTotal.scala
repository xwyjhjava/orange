package com.xiaoi.feature

import com.xiaoi.common.{HadoopOpsUtil, StatsUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

/**
  * created by liusiyuan on 2018/7/2
  * step_5_1to3   ui基本统计  ui_total;   ui money相关特征   ui_money_total;
  * ui 共现特征  ui_cooccur_size_sta
  */

object uiTotal {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("uiTotal")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext

    HadoopOpsUtil.removeDir(params.ui_total_output_path, params.ui_total_output_path)
    HadoopOpsUtil.removeDir(params.ui_money_total_output_path, params.ui_money_total_output_path)
    HadoopOpsUtil.removeDir(params.ui_cooccur_size_sta_output_path,
      params.ui_cooccur_size_sta_output_path)

    //uid, sldat, user_id，item_id，dptno, qty, amt
    // 准备数据， data_simplified
    val data_simplified = get_data_simplified(params.data_simplified_input_path, sc)
    data_simplified.cache()

    //user_id, first_order_time, log_count, item_count, basket_count, money
    // 准备数据， user_total
    val user_total = get_user_total(params.user_total_input_path, sc)

    logger.info(s"ui_total write: ${params.ui_total_output_path}")
    // step_5_1
    /**
     *
     * user_id
     * item_id
     * total_logs
     * total_logs_ratio
     * total_sessions
     * total_session_ratio
     *
     */
    val ui_total = get_ui_total(data_simplified, user_total)
    ui_total.saveAsTextFile(params.ui_total_output_path)

    logger.info("ui_money_total data write...")
    // step_5_2
    /**
     * user_id
     * item_id
     * total_money
     * total_money_rank_percent
     * avg_price_rank_percent
     */
    val ui_money_total = get_ui_money_total(data_simplified)
    ui_money_total.saveAsTextFile(params.ui_money_total_output_path)

    logger.info("ui_cooccur_size_sta data write...")
    //step_5_3
    /**
     * user_id
     * item_id
     * min
     * max
     * median
     * avg
     * std
     */
    val ui_cooccur_size_sta = get_ui_basket_log_count(data_simplified)
    ui_cooccur_size_sta.saveAsTextFile(params.ui_cooccur_size_sta_output_path)

    sc.stop()
  }

  def get_data_simplified(data_simplified_input_path: String, sc: SparkContext):
      RDD[(String, String, String, String, String, String, String)] = {
    sc.textFile(data_simplified_input_path)
      .map(line => {
        val strArr = line.split("\\|")
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
  }

  def get_user_total(user_total_input_path: String, sc: SparkContext):
      RDD[(String, String, String, String, String, String)] = {
    val user_total = sc.textFile(user_total_input_path).map(line => {
      val strArr = line.split("\\|")
      val user_id = strArr(0)
      val first_order_time = strArr(1)
      val log_count = strArr(2)
      val item_count = strArr(3)
      val basket_count = strArr(4)
      val money = strArr(5)
      (user_id, first_order_time, log_count, item_count, basket_count, money)
    })
    user_total
  }

  def get_ui_total(data_simplified: RDD[(String, String, String, String, String, String, String)],
                   user_total: RDD[(String, String, String, String, String, String)]): RDD[(String)] = {
    val ui_total_logs_and_sessions = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) => ((user_id, item_id), uid) }
      .groupByKey().mapValues(x => {
        val uid_list = x.toList
        val total_logs = uid_list.length
        val total_sessions = uid_list.distinct.length
        (total_logs, total_sessions)
    }).map { case ((user_id, item_id), (total_logs, total_sessions)) =>
      (user_id, (item_id, total_logs, total_sessions))
    }
    val ui_total_join = user_total
      .map { case (user_id, first_order_time, log_count, item_count, basket_count, money) =>
        (user_id, (log_count.toDouble, basket_count.toDouble))
      }
      .join(ui_total_logs_and_sessions)

    val ui_total = ui_total_join
      .map { case (user_id, ((log_count, basket_count), (item_id, total_logs, total_sessions))) =>
        user_id + "|" + item_id + "|" + total_logs + "|" + total_logs / log_count +
          "|" + total_sessions + "|" + total_sessions / basket_count
      }
    ui_total
  }

  //该用户、该商品的消费总金额及排名（同一商品的所有用户的总金额从高到低）
  def get_ui_money_total(data_simplified: RDD[(String, String, String, String, String, String, String)]): 
      RDD[(String)] = {
    val ui_total_money = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) => ((user_id, item_id), amt.toDouble) }
      .reduceByKey(_ + _)
      .map { case ((user_id, item_id), total_money) => (item_id, (total_money, user_id)) }
      .groupByKey()
      .mapValues(x => {
        val values = x.toList
        //按照total_money从大到小排列
        val sort_total_money = values.sortWith((x, y) => (x._1 > y._1))
        val index_list = sort_total_money.zipWithIndex //List(((3,m),0), ((2,b),1), ((1,a),2))
        //同一件商品所有用户的(total_money, user_id),这个商品的用户数
        (index_list, index_list.length) })
      .flatMap(x => {
         val item_id = x._1
         val index_list = x._2._1
         val item_user_count = x._2._2
         //因为索引是从0开始的，所以要加1，下同
         index_list.map { case ((total_money, user_id), index) =>
           ((user_id, item_id), (total_money, (index + 1).toDouble / item_user_count))} })

      //该用户、该商品的平均价格,及在该用户中的排名（从高到低）
    val ui_avg_price = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        ((user_id, item_id), amt.toDouble / qty.toDouble)
      }
      .groupByKey()
      .map(x => {
        val user_id = x._1._1
        val item_id = x._1._2
        val avg_array = x._2.toArray
        val avg = StatsUtil.mean(avg_array)
        (user_id, (avg, item_id)) })
      .groupByKey()
      .mapValues(x => {
        val values = x.toList
        //按照avg从大到小排列
        val sort_avg = values.sortWith((x, y) => (x._1 > y._1))
        val index_list = sort_avg.zipWithIndex //List(((3,m),0), ((2,b),1), ((1,a),2))
        //同用户的所有商品(avg,item_id),这个用户的商品数
        (index_list, index_list.length) })
      .flatMap(x => {
        val user_id = x._1
        val index_list = x._2._1
        val user_item_count = x._2._2
        //因为索引是从0开始的，所以要加1，下同
        index_list.map {
          case ((avg, item_id), index) => ((user_id, item_id), (index + 1).toDouble / user_item_count)
        }
      })

    //ui_money_total 最终文件合并
    val ui_money_total = ui_total_money.join(ui_avg_price)
      .map {
        case ((user_id, item_id), ((total_money, total_money_rank_percent), avg_price_rank_percent)) =>
        user_id + "|" + item_id + "|" + total_money + "|" + total_money_rank_percent +
          "|" + avg_price_rank_percent
      }
    ui_money_total
  }

  // ui 共现特征  ui_cooccur_size_
  def get_ui_basket_log_count(data_simplified:
      RDD[(String, String, String, String, String, String, String)]): RDD[(String)] = {
    val ui_basket_logs_count = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) => ((uid, user_id), 1) }
      .reduceByKey(_ + _)
      .map { case ((uid, user_id), basket_logs_count) => ((uid, user_id), basket_logs_count) }
    val ui_items_logs_count = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) => ((uid, user_id, item_id), 1) }
      .reduceByKey(_ + _)
      .map { case ((uid, user_id, item_id), items_logs_count) =>
        ((uid, user_id), (item_id, items_logs_count))
      }
    val ui_cooccur_size_sta = ui_items_logs_count.join(ui_basket_logs_count)
      .map { case ((uid, user_id), ((item_id, items_logs_count), basket_logs_count)) =>
        ((user_id, item_id), (basket_logs_count - items_logs_count).toDouble)
      }
      .groupByKey()
      .map(x => {
        val user_id = x._1._1
        val item_id = x._1._2
        val cooccur_count_arr = x._2.toArray
        user_id + "|" + item_id + "|" + StatsUtil.sta_Count(cooccur_count_arr)
      })
    ui_cooccur_size_sta
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_5") {
      opt[String]("data_simplified_input_path")
        .action((x, c) => c.copy(data_simplified_input_path = x))
      opt[String]("user_total_input_path")
        .action((x, c) => c.copy(user_total_input_path = x))
      opt[String]("ui_total_output_path")
        .action((x, c) => c.copy(ui_total_output_path = x))
      opt[String]("ui_money_total_output_path")
        .action((x, c) => c.copy(ui_money_total_output_path = x))
      opt[String]("ui_cooccur_size_sta_output_path")
        .action((x, c) => c.copy(ui_cooccur_size_sta_output_path = x))
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
                     user_total_input_path: String = "data/step_3/user_total",
                     ui_total_output_path: String = "data/step_5/ui_total",
                     ui_money_total_output_path: String = "data/step_5/ui_money_total",
                     ui_cooccur_size_sta_output_path: String = "data/step_5/ui_cooccur_size_sta"
                   )

}
