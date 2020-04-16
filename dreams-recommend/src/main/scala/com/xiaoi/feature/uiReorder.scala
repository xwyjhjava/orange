package com.xiaoi.feature

import java.text.SimpleDateFormat
import java.util

import com.xiaoi.common.{HadoopOpsUtil, StatsUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/**
  * created by liusiyuan on 2018/7/2
  * step_5_5 ui  reorder特征     todo： 特征比较碎，可以整合，提高IO效率
  */
object uiReorder {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("step_5_5")
    val sc = new SparkContext(conf)
    HadoopOpsUtil.removeDir(params.ui_reorder_count_output_path, params.ui_reorder_count_output_path)
    HadoopOpsUtil.removeDir(params.ui_reorder_ratio_output_path, params.ui_reorder_ratio_output_path)
    HadoopOpsUtil.removeDir(params.ui_reorder_days_between_orders_sta_output_path,
      params.ui_reorder_days_between_orders_sta_output_path)
    HadoopOpsUtil.removeDir(params.ui_reorder_orders_between_orders_sta_output_path,
      params.ui_reorder_orders_between_orders_sta_output_path)
    HadoopOpsUtil.removeDir(params.ui_reorder_time_dow_output_path,
      params.ui_reorder_time_dow_output_path)

    //user_id，item_id，basket_list
    // 准备数据  user_basket_has_item_list
    val user_baskets_has_item_list = get_user_baskets_items(
      params.user_baskets_has_item_list_input_path, sc)

    //uid, sldat, user_id，item_id，dptno, qty, amt
    // 准备数据  data_simplified
    val data_simplified = get_data_simplified(
      params.data_simplified_input_path, sc)
    data_simplified.cache()

    // step_5_5 ui_reorder
    logger.info("ui_reorder_count write...")
    /**
     * user_id
     * item_id
     * count
     */
    val ui_reorder_count = user_baskets_has_item_list._1
    ui_reorder_count.repartition(1).saveAsTextFile(params.ui_reorder_count_output_path)

    logger.info("ui_reorder_ratio write...")
    /**
     * user_id
     * item_id
     * ratio
     */
    val ui_reorder_ratio = user_baskets_has_item_list._2
    ui_reorder_ratio.repartition(1).saveAsTextFile(params.ui_reorder_ratio_output_path)



    val ui_reorder_time = get_ui_reorder_time(data_simplified)
    logger.info("ui_reorder_days_between_orders_sta data...")
    /**
     * user_id
     * item_id
     * min
     * max
     * median
     * avg
     * std
     */
    val ui_reorder_days_between_orders_sta = ui_reorder_time._1
    ui_reorder_days_between_orders_sta.repartition(1)
      .saveAsTextFile(params.ui_reorder_days_between_orders_sta_output_path)

    logger.info("ui_reorder_time_dow write...")
    /**
     * user_id
     * item_id
     * dow1
     * dow2
     * dow3
     * dow4
     * dow5
     * dow6
     * dow7
     *
     */
    val ui_reorder_time_dow = ui_reorder_time._2
    ui_reorder_time_dow.repartition(1)
      .saveAsTextFile(params.ui_reorder_time_dow_output_path)

    logger.info("ui_reorder_orders_between_orders_sta write...")
    /**
     * user_id
     * item_id
     * min
     * max
     * median
     * avg
     * std
     *
     */
    val ui_reorder_orders_between_orders_sta = get_between_orders_sta(data_simplified)
    ui_reorder_orders_between_orders_sta.repartition(1)
      .saveAsTextFile(params.ui_reorder_orders_between_orders_sta_output_path)

    sc.stop()
  }

  def get_user_baskets_items(user_baskets_has_item_list_input_path: String,
                             sc: SparkContext) = {
    val user_baskets_has_item_list = sc.textFile(user_baskets_has_item_list_input_path).map(
      line => {
        val strArr = line.split("\\|")
        val user_id = strArr(0)
        val item_id = strArr(1)
        val basket_list = strArr.drop(2)
        val ui_count = basket_list.filter(_.equals("1")).length
        val reorder_count = if (ui_count >= 1) ui_count - 1 else 0
        //第一次复购之后的购物次数
        val reorder_to_end_count = basket_list.length - basket_list.indexOf("1") - 1
        val reorder_ratio = if (reorder_to_end_count == 0) 0 else reorder_count / reorder_to_end_count.toDouble
        (user_id, item_id, reorder_count, reorder_ratio)
      })
      (user_baskets_has_item_list.map(x => x._1 + "|" + x._2 + "|" + x._3),
      user_baskets_has_item_list.map(x => x._1 + "|" + x._2 + "|" + x._4))
  }

  def get_data_simplified(data_simplified_input_path: String,
                          sc: SparkContext) = {
    val data_simplified = sc.textFile(data_simplified_input_path).map(line => {
      val strArr = line.split("\\|")
      val uid = strArr(0)
      val sldat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(strArr(1)).getTime
      val user_id = strArr(2)
      val item_id = strArr(3)
      //      ((uid,user_id,item_id),sldat)
      (uid, (user_id, item_id, sldat))
    }).filter(x => {
      !x._2._1.equals("")
    })
      .groupByKey()
      .mapValues(x => {
        //交易回合中取第一时间
        val sort_sldat = x.toArray.sortBy(_._3)
        val min_sldat = sort_sldat(0)._3
        x.map { case (user_id, item_id, sldat) => (user_id, item_id, min_sldat) }
      }).flatMap(x => {
      val uid = x._1
      x._2.map { case (user_id, item_id, sldat) => ((uid, user_id, item_id), sldat) }
    })
    data_simplified
  }

  def get_ui_reorder_time(data_simplified: RDD[((String, String, String), Long)]) = {
    val ui_reorder_time = data_simplified
      .map { case ((uid, user_id, item_id), sldat) => ((user_id, item_id), sldat) }
      .distinct()
      .groupByKey()
      .mapValues(x => {
        val time_array = x.toArray
        val sort_time = time_array.sortWith(_ < _)
        //从小到大排列
        //相邻两次购物的时间差（天）
        val days_between_array = sort_time.sliding(2).toArray
          .map(x => if (x.length > 1) (x(1) - x(0)).toDouble / 86400000 else 0)
        val sta = StatsUtil.sta_Count(days_between_array)
        val reorder_time_array = sort_time.drop(1)
        //把第一次购买该商品的时间去掉
        val reorder_time_dow = week_count(reorder_time_array)
        val week_count_list = ListBuffer[Int]()
        //1-7 星期几
        for (i <- 1 to 7) {
          val week_count = reorder_time_dow.get(i)
          week_count_list.append(week_count)
        }
        //两次购物时间差的sta，复购分别在周几的次数
        (sta, week_count_list)
      }).map { case ((user_id, item_id), (sta, week_count_list)) =>
      (user_id, item_id, sta, week_count_list.mkString("|"))
    }
    (ui_reorder_time.map(x => x._1 + "|" + x._2 + "|" + x._3),
      ui_reorder_time.map(x => x._1 + "|" + x._2 + "|" + x._4))
  }

  def get_between_orders_sta(data_simplified: RDD[((String, String, String), Long)]) = {
    val ui_time = data_simplified.map {
      case ((uid, user_id, item_id), sldat) => ((user_id, sldat), item_id)
    }.distinct()
    val user_basket_index = ui_time.map { case ((user_id, sldat), item_id) => (user_id, sldat) }
      .distinct()
      .groupByKey()
      .mapValues(x => {
        val sldat_list = x.toList
        val sort_index_list = sldat_list.sortWith(_ < _).zipWithIndex //从0开始的索引
        sort_index_list
      }).flatMap(x => {
      val user_id = x._1
      val sort_index_list = x._2
      sort_index_list.map { case (max_sldat, index) => ((user_id, max_sldat), index) }
    })

    val ui_reorder_orders_between_orders_sta = ui_time.join(user_basket_index)
      .map { case ((user_id, max_sldat), (item_id, index)) => ((user_id, item_id), index) }
      .groupByKey().map(x => {
      val user_id = x._1._1
      val item_id = x._1._2
      val index_array = x._2.toArray
      val index_between_array = index_array
        .sortWith(_ < _).sliding(2).toArray
        .map(x => if (x.length > 1) (x(1) - x(0)).toDouble else 0)
      val sta = StatsUtil.sta_Count(index_between_array)
      user_id + "|" + item_id + "|" + sta
    })
    ui_reorder_orders_between_orders_sta
  }

  //一个星期中每天的交易记录数量
  def week_count(arr: Array[Long]) = {
    val week_map = new util.HashMap[Int, Int]()
    //1-7分别代表星期一 ==>星期日
    for (i <- 1 to 7) {
      week_map.put(i, 0)
    }
    arr.map(x => {
      //星期几
      val week = new SimpleDateFormat("E").format(x)
      week match {
        case "星期一" => week_map.put(1, week_map.get(1) + 1)
        case "星期二" => week_map.put(2, week_map.get(2) + 1)
        case "星期三" => week_map.put(3, week_map.get(3) + 1)
        case "星期四" => week_map.put(4, week_map.get(4) + 1)
        case "星期五" => week_map.put(5, week_map.get(5) + 1)
        case "星期六" => week_map.put(6, week_map.get(6) + 1)
        case "星期日" => week_map.put(7, week_map.get(7) + 1)
        case _ => 0
      }
    })
    week_map
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_3") {
      opt[String]("user_baskets_has_item_list_input_path")
        .action((x, c) => c.copy(user_baskets_has_item_list_input_path = x))
      opt[String]("data_simplified_input_path")
        .action((x, c) => c.copy(data_simplified_input_path = x))
      opt[String]("ui_reorder_count_output_path")
        .action((x, c) => c.copy(ui_reorder_count_output_path = x))
      opt[String]("ui_reorder_ratio_output_path")
        .action((x, c) => c.copy(ui_reorder_ratio_output_path = x))
      opt[String]("ui_reorder_days_between_orders_sta_output_path")
        .action((x, c) => c.copy(ui_reorder_days_between_orders_sta_output_path = x))
      opt[String]("ui_reorder_orders_between_orders_sta_output_path")
        .action((x, c) => c.copy(ui_reorder_orders_between_orders_sta_output_path = x))
      opt[String]("ui_reorder_time_dow_output_path")
        .action((x, c) => c.copy(ui_reorder_time_dow_output_path = x))
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
                     user_baskets_has_item_list_input_path: String = "data/step_3/user_baskets_has_item_list",
                     data_simplified_input_path: String = "data/step_3/data_simplified",
                     ui_reorder_count_output_path: String = "data/step_5/ui_reorder_count",
                     ui_reorder_ratio_output_path: String = "data/step_5/ui_reorder_ratio",
                     ui_reorder_days_between_orders_sta_output_path: String = "data/step_5/ui_reorder_days_between_orders_sta",
                     ui_reorder_orders_between_orders_sta_output_path: String = "data/step_5/ui_reorder_orders_between_orders_sta",
                     ui_reorder_time_dow_output_path: String = "data/step_5/ui_reorder_time_dow"
                   )

}
