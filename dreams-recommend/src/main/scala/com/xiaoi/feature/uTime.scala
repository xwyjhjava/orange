package com.xiaoi.feature

import com.xiaoi.common.{DateUtil, HadoopOpsUtil, StatsUtil, StrUtil}
import com.xiaoi.conf.ConfigurationManager
import com.xiaoi.constant.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/**
  * created by josh on 2018/7/2
  * 用户时间特征，主要是一些时间分布： 小时，星期，工作日
  */
object uTime {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("step_5_24 or uTime")
    val sc = new SparkContext(conf)
    HadoopOpsUtil.removeDir(params.u_order_days_ratio_output_path, params.u_order_days_ratio_output_path)
    HadoopOpsUtil.removeDir(params.u_time_hour_slot_ratio_output_path, params.u_time_hour_slot_ratio_output_path)
    HadoopOpsUtil.removeDir(params.u_time_dow_ratio_output_path, params.u_time_dow_ratio_output_path)
    HadoopOpsUtil.removeDir(params.u_time_workday_ratio_output_path, params.u_time_workday_ratio_output_path)

    logger.info("u_order_days_ratio: 字段： user_id, ratio")
    val u_order_days_ratio = get_order_days_ratio(sc, params.data_14cols_1_input_path,
      params.observe_time_enabled,params.observe_time)
    u_order_days_ratio.map(StrUtil.tupleToString(_, "|"))
      .repartition(1).saveAsTextFile(params.u_order_days_ratio_output_path)

    logger.info("u_time_hour_slot_ratio	字段：user_id, w1, w2, w3, w3, w5, w6")
    val u_time_hour_ratio = get_time_hour_ratio(params.ui_time_hour_input_path, sc)
    u_time_hour_ratio.map(StrUtil.tupleToString(_, "|"))
      .repartition(1).saveAsTextFile(params.u_time_hour_slot_ratio_output_path)

    logger.info("u_time_dow_ratio  字段：user_id, w1, w2, w3, w3, w5, w6, w7")
    val u_time_dow_ratio = get_time_dow_ratio(params.ui_time_dow_input_path, sc)
    u_time_dow_ratio.map(StrUtil.tupleToString(_, "|"))
      .repartition(1).saveAsTextFile(params.u_time_dow_ratio_output_path)

    logger.info("u_time_workday_ratio  字段：user_id, ratio")
    val u_time_workday_ratio = get_time_workday_ratio(params.ui_time_workday_input_path, sc)
    u_time_workday_ratio.repartition(1)
      .saveAsTextFile(params.u_time_workday_ratio_output_path)
    sc.stop()
  }

  // 该用户进行购物的日期数/ 从该用户第一次购物算起的日期数
  def get_order_days_ratio(sc: SparkContext,
                           data_14cols_path: String,
                           observe_time_enabled: Boolean,
                           observe_time: String)={

    val data_14cols_1 = sc.textFile(data_14cols_path).map { line =>
      val tokens = line.split("\\|", -1)
      val UID = tokens(ConfigurationManager.getInt(Constants.FIELD_UID))
      val SLDAT = tokens(ConfigurationManager.getInt(Constants.FIELD_SLDAT))
      val user_id = tokens(ConfigurationManager.getInt(Constants.FIELD_VIPID))
      (UID, SLDAT, user_id)
    }.cache()
    val observe_day = if (observe_time_enabled) observe_time else {
      data_14cols_1.map(_._2)
        .max()
        .slice(0, 10)
    }
    val days_ratio = data_14cols_1.map{ case (uid, sldat, user_id) => ((user_id,uid), sldat)}
      .groupByKey()
      .mapValues(_.toList.min)
      .map(x => (x._1._1, x._2.slice(0,10))) //(user, date)
      .groupByKey()
      .mapValues(dateList => {
        val first_day_order = dateList.min
        val time_prefix = " 00:00:00"
        val days_from_start = DateUtil.dateTimeDiffInDay(
          first_day_order + time_prefix, observe_day + time_prefix)
        if(days_from_start == 0) 0.0
        else StatsUtil.double_ratio(dateList.toList.distinct.size, days_from_start)
    })
    data_14cols_1.unpersist()
    days_ratio
  }

  // 计算时间分布
  def user_temporal_dist(sc: SparkContext,
                         readPath: String,
                         start_index: Int, end_index: Int): RDD[(String, String)] = {

    val count_dist = sc.textFile(readPath).map{ line =>
      val tokens = line.split("\\|",-1)
      val user_id = tokens(0)
      val dow_count = tokens.drop(2).map(_.toDouble)
      (user_id, dow_count)
    }.groupByKey().mapValues(time_count => {
      var count_concat = ListBuffer[Double]()
      for(dow <- start_index to end_index) {
        count_concat += time_count.toList.map(x =>x(dow)).sum
      }
      val all_count = count_concat.sum
      count_concat.map(StatsUtil.double_ratio(_,all_count)).mkString("|")
    })
    count_dist
  }


  def get_time_hour_ratio(ui_time_hour_input_path: String,
                          sc: SparkContext) = {
    user_temporal_dist(sc, ui_time_hour_input_path,0, 23-6)
  }

  def get_time_dow_ratio(ui_time_dow_input_path: String, sc: SparkContext) = {
    user_temporal_dist(sc, ui_time_dow_input_path, 0, 7-1)
  }

  def get_time_workday_ratio(ui_time_workday_input_path: String, sc: SparkContext) = {
    val workday_count = sc.textFile(ui_time_workday_input_path).map{line =>
      val tokens = line.split("\\|",-1)
      val user_id = tokens(0)
      val workday_count = tokens(2).toDouble
      val item_count = tokens(4).toDouble
      (user_id, (workday_count, item_count))
    }
    workday_count.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .map(x => {
        val user_id = x._1
        val count_sum = x._2
        val ratio = StatsUtil.double_ratio(count_sum._1, count_sum._2)
        user_id +"|"+ ratio
      })
  }


  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_5_24") {
      opt[String]("data_14cols_1_input_path")
        .action((x, c) => c.copy(data_14cols_1_input_path = x))
      opt[String]("ui_time_dow_input_path")
        .action((x, c) => c.copy(ui_time_dow_input_path = x))
      opt[String]("ui_time_hour_input_path")
        .action((x, c) => c.copy(ui_time_hour_input_path = x))
      opt[String]("ui_time_workday_input_path")
        .action((x, c) => c.copy(ui_time_workday_input_path = x))
      opt[Boolean]("observe_time_enabled")
        .action((x, c) => c.copy(observe_time_enabled = x))
      opt[String]("observe_time")
        .action((x, c) => c.copy(observe_time = x))
      opt[String]("u_order_days_ratio_output_path")
        .action((x, c) => c.copy(u_order_days_ratio_output_path = x))
      opt[String]("u_time_hour_slot_ratio_output_path")
        .action((x, c) => c.copy(u_time_hour_slot_ratio_output_path = x))
      opt[String]("u_time_dow_ratio_output_path")
        .action((x, c) => c.copy(u_time_dow_ratio_output_path = x))
      opt[String]("u_time_workday_ratio_output_path")
        .action((x, c) => c.copy(u_time_workday_ratio_output_path = x))
      checkConfig { params =>
        success
      }
    }
    parser.parse(args, defaultParams).map{params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }
  case class Params(
                     data_14cols_1_input_path: String = "",
                     ui_time_dow_input_path:String = "data/step_5/ui_time_dow",
                     ui_time_hour_input_path:String = "data/step_5/ui_time_hour",
                     ui_time_workday_input_path:String = "data/step_5/ui_time_workday",
                     observe_time_enabled: Boolean = false,
                     observe_time: String = "",
                     u_order_days_ratio_output_path: String = "",
                     u_time_hour_slot_ratio_output_path: String = "",
                     u_time_dow_ratio_output_path: String = "",
                     u_time_workday_ratio_output_path: String = ""
                   )
}
