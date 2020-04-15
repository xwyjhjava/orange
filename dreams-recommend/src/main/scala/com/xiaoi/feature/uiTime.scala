package com.xiaoi.feature

import java.text.SimpleDateFormat

import com.xiaoi.common.{DateUtil, HadoopOpsUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.{ListBuffer, _}
/*
 * ui 时间特征，基于时间维度的统计分布 
 */

object uiTime {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("step_5_4 or uiTime")
    val sc = new SparkContext(conf)
    HadoopOpsUtil.removeDir(params.ui_time_dow_output_path, params.ui_time_dow_output_path)
    HadoopOpsUtil.removeDir(params.ui_time_hour_output_path, params.ui_time_hour_output_path)
    HadoopOpsUtil.removeDir(params.ui_time_month_output_path, params.ui_time_month_output_path)
    HadoopOpsUtil.removeDir(params.ui_time_season_output_path, params.ui_time_season_output_path)
    HadoopOpsUtil.removeDir(params.ui_time_workday_output_path, params.ui_time_workday_output_path)

    //uid, sldat, user_id，item_id，dptno, qty, amt
    val data_simplified = get_data_simplified(params. data_simplified_input_path, sc)
    data_simplified.cache()
    val ui_time_count = get_ui_time_count(data_simplified)
    ui_time_count.cache()

    logger.info(s"ui_time_dow write: ${params.ui_time_dow_output_path}")
    val ui_time_dow = get_ui_time_hour(ui_time_count)
    ui_time_dow.saveAsTextFile(params.ui_time_dow_output_path)

    logger.info("ui_time_hour write...")
    val ui_time_hour = get_ui_data_hour(ui_time_count)
    ui_time_hour.saveAsTextFile(params.ui_time_hour_output_path)

    logger.info("ui_time_month write...")
    val ui_time_month = get_ui_time_month(ui_time_count)
    ui_time_month.saveAsTextFile(params.ui_time_month_output_path)

    logger.info("ui_time_season write...")
    val ui_time_season = get_ui_time_season(ui_time_count)
    ui_time_season.saveAsTextFile(params.ui_time_season_output_path)

    logger.info("ui_time_workday write...")
    val workday_holiday_rdd = get_workday_holiday(params.calendar_input_path, sc)
    val ui_time_workday = get_ui_time_workday(data_simplified, workday_holiday_rdd)
    ui_time_workday.saveAsTextFile(params.ui_time_workday_output_path)

    sc.stop()
  }

  def get_data_simplified(data_simplified_input_path: String, sc: SparkContext):
      RDD[(String, String, String, String, String, String, String)] = {
    val data_simplified = sc.textFile(data_simplified_input_path).map(line => {
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
    data_simplified
  }

  def get_workday_holiday(calendar_input_path: String,
                          sc: SparkContext): RDD[(String, String)] = {
    sc.textFile(calendar_input_path).map(line => {
      val strArr = line.split("\\|")
      val date = strArr(0)
      val isWorkday = strArr(1)
      (date, isWorkday)
    })
  }

  def get_ui_time_count(data_simplified:
                        RDD[(String, String, String, String, String, String, String)]) = {

    val ui_time_count = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        ((user_id, item_id), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(sldat).getTime)
      }
      .groupByKey().map(x => {
      val user_id = x._1._1
      val item_id = x._1._2
      val sldat_array = x._2.toArray
      val ui_count = sldat_array.length
      val week_hour_month_season_hashMap = DateUtil.time_count(sldat_array)
      //用户id，商品id，星期_小时_月份_季节的hashMap组合，该用户、该商品的购物次数
      (user_id, item_id, week_hour_month_season_hashMap, ui_count)
    })
    ui_time_count
  }

  def get_ui_time_hour(ui_time_count:
      RDD[(String, String, (Map[Int, Int], Map[Int, Int], Map[Int, Int], Map[Int, Int]), Int)]) = {

    val ui_time_dow = ui_time_count.map(x => {
      val user_id = x._1
      val item_id = x._2
      val week_count_hashMap = x._3._1
      val ui_count = x._4.toDouble
      val week_count_list = ListBuffer[Int]()
      val week_count_ratio_list = ListBuffer[Double]()
      //1-7 星期几
      for (i <- 1 to 7) {
        val week_count = week_count_hashMap.get(i).get
        week_count_list.append(week_count)
        week_count_ratio_list.append(week_count / ui_count)
      }
      user_id + "|" + item_id + "|" + week_count_list.mkString("|") + "|" +
        week_count_ratio_list.mkString("|")
    })
    ui_time_dow
  }

  def get_ui_data_hour(ui_time_count:
        RDD[(String, String, (Map[Int, Int], Map[Int, Int], Map[Int, Int], Map[Int, Int]), Int)]) = {

    val ui_time_hour = ui_time_count.map(x => {
      val user_id = x._1
      val item_id = x._2
      val hour_count_hashMap = x._3._2
      val ui_count = x._4.toDouble
      val hour_count_list = ListBuffer[Int]()
      val hour_count_ratio_list = ListBuffer[Double]()
      //6-23 小时
      for (i <- 6 until 24) {
        val hour_count = hour_count_hashMap.get(i).get
        hour_count_list.append(hour_count)
        hour_count_ratio_list.append(hour_count / ui_count)
      }
      user_id + "|" + item_id + "|" + hour_count_list.mkString("|") + "|" +
        hour_count_ratio_list.mkString("|")
    })
    ui_time_hour
  }

  def get_ui_time_month(ui_time_count:
        RDD[(String, String, (Map[Int, Int], Map[Int, Int], Map[Int, Int], Map[Int, Int]), Int)]) = {

    val ui_time_month = ui_time_count.map(x => {
      val user_id = x._1
      val item_id = x._2
      val month_count_hashMap = x._3._3
      val ui_count = x._4.toDouble
      val month_count_list = ListBuffer[Int]()
      val month_count_ratio_list = ListBuffer[Double]()
      //1-12 月份
      for (i <- 1 to 12) {
        val month_count = month_count_hashMap.get(i).get
        month_count_list.append(month_count)
        month_count_ratio_list.append(month_count / ui_count)
      }
      user_id + "|" + item_id + "|" + month_count_list.mkString("|") + "|" +
        month_count_ratio_list.mkString("|")
    })
    ui_time_month
  }

  def get_ui_time_season(ui_time_count:
        RDD[(String, String, (Map[Int, Int], Map[Int, Int], Map[Int, Int], Map[Int, Int]), Int)]) = {
    val ui_time_season = ui_time_count.map(x => {
      val user_id = x._1
      val item_id = x._2
      val season_count_hashMap = x._3._4
      val ui_count = x._4.toDouble
      val season_count_list = ListBuffer[Int]()
      val season_count_ratio_list = ListBuffer[Double]()
      //1-4 代表四季
      for (i <- 1 to 4) {
        val season_count = season_count_hashMap.get(i).get
        season_count_list.append(season_count)
        season_count_ratio_list.append(season_count / ui_count)
      }
      user_id + "|" + item_id + "|" + season_count_list.mkString("|") + "|" +
        season_count_ratio_list.mkString("|")
    })
    ui_time_season
  }

  def get_ui_time_workday(data_simplified:
                          RDD[(String, String, String, String, String, String, String)],
                          workday_holiday_rdd: RDD[(String, String)]) = {
    val ui_time_workday = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        (sldat.split(" ")(0), (user_id, item_id))
      }
      .join(workday_holiday_rdd)
      .map { case (date, ((user_id, item_id), isOrNoWorkday)) =>
        ((user_id, item_id), isOrNoWorkday)
      }
      .groupByKey()
      .map(x => {
        val user_id = x._1._1
        val item_id = x._1._2
        val isOrNoWorkday_array = x._2.toArray
        val workday_count = isOrNoWorkday_array.filter(x => x.equals("0")).length
        val item_count = isOrNoWorkday_array.length
        user_id + "|" + item_id + "|" + workday_count + "|" +
          workday_count.toDouble / item_count + "|" + item_count
      })
    ui_time_workday
  }


  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_3") {
      opt[String]("data_simplified_input_path")
        .action((x, c) => c.copy(data_simplified_input_path = x))
      opt[String]("calendar_input_path")
        .action((x, c) => c.copy(calendar_input_path = x))
      opt[String]("ui_time_dow_output_path")
        .action((x, c) => c.copy(ui_time_dow_output_path = x))
      opt[String]("ui_time_hour_output_path")
        .action((x, c) => c.copy(ui_time_hour_output_path = x))
      opt[String]("ui_time_month_output_path")
        .action((x, c) => c.copy(ui_time_month_output_path = x))
      opt[String]("ui_time_season_output_path")
        .action((x, c) => c.copy(ui_time_season_output_path = x))
      opt[String]("ui_time_workday_output_path")
        .action((x, c) => c.copy(ui_time_workday_output_path = x))
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
                     calendar_input_path: String = "data/calendar/2017",
                     ui_time_dow_output_path: String = "data/step_5/ui_time_dow",
                     ui_time_hour_output_path: String = "data/step_5/ui_time_hour",
                     ui_time_month_output_path: String = "data/step_5/ui_time_month",
                     ui_time_season_output_path: String = "data/step_5/ui_time_season",
                     ui_time_workday_output_path: String = "data/step_5/ui_time_workday"
                   )

}
