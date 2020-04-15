package com.xiaoi.feature

import com.xiaoi.common.HadoopOpsUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.Map

/* 
 * 用户购买商品的类别分布（限最畅销的10个类别），趋势   最经常购买的5种类别
 * */


object uClass {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("step_5_26to27 or uClass")
    val sc = new SparkContext(conf)
    remove_exist_dir(params)
    logger.info("step_5_26to27 中间数据生成......")

    val user_info_rdd = get_user_info(sc, params.data_simplified_input_path,
      params.class_top10_input_path,
      params.user_frequency_month_input_path,
      params.topn_class_ratio,
      params.delta)
    val data_simplified = user_info_rdd._1
    val topn_class_rdd = user_info_rdd._2
    val u_trend = user_info_rdd._3
    val user_class = user_info_rdd._4

    logger.info("u_distinct_classes saving...... ")
    val u_distinct_classes = get_u_distinct_classes(data_simplified, user_class)
    u_distinct_classes.repartition(1)
      .saveAsTextFile(params.u_distinct_classes_output_path)

    val user_class_count = get_user_class_count(user_class)

    logger.info("u_class_ratio saving...... ")
    val u_class_ratio = get_u_class_ratio(data_simplified, user_class_count,
      topn_class_rdd, params.topn_class_ratio)
    u_class_ratio.repartition(1).saveAsTextFile(params.u_class_ratio_output_path)

    logger.info("u_classes_top5 saving...... ")
    val u_classes_top5 = get_u_classes_top5(user_class_count, params.topn_class)
    u_classes_top5.repartition(1).saveAsTextFile(params.u_classes_top5_output_path)

    logger.info("u_trend saving...... ")
    u_trend.repartition(1).saveAsTextFile(params.u_trend_output_path)

    sc.stop()
  }

  def remove_exist_dir(params: Params): Unit = {
    HadoopOpsUtil.removeDir(params.u_distinct_classes_output_path,
      params.u_distinct_classes_output_path)
    HadoopOpsUtil.removeDir(params.u_class_ratio_output_path,
      params.u_class_ratio_output_path)
    HadoopOpsUtil.removeDir(params.u_classes_top5_output_path, params.u_classes_top5_output_path)
    HadoopOpsUtil.removeDir(params.u_trend_output_path, params.u_trend_output_path)
  }

  def get_user_info(sc: SparkContext,
                    data_simplified_input_path: String,
                    class_top10_input_path: String,
                    user_frequency_month_input_path: String,
                    topn_class_ratio: Int,
                    delta: Double
                   ) = {
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

    val user_class = data_simplified.map {
      case (uid, sldat, user_id, item_id, dptno, qty, amt) => (user_id, dptno)
    }

    //dptno， dptname，  class_ratio
    val topn_class = sc.textFile(class_top10_input_path)
      .zipWithIndex()
      .map(x => {
        val index = x._2
        val strArr = x._1.split("\\|", -1)
        val dptno = strArr(0)
        val class_ratio = strArr(2)
        ((class_ratio.toDouble, dptno), index)
      })

    //取出最畅销的商品类别前10种
    val topn_class_ratio_rdd = topn_class
      .filter(x => x._2 < topn_class_ratio)
      .map { case ((class_ratio, dptno), index) => (dptno, index) }

    val u_trend = sc.textFile(user_frequency_month_input_path).map(line => {
      val strArr = line.split("\\|", -1)
      val user_id = strArr(0)
      val month_1 = strArr(1).toDouble
      val month_2 = strArr(2).toDouble
      val month_3 = strArr(3).toDouble
      val month_4 = strArr(4).toDouble
      val month_5 = strArr(5).toDouble
      val month_6 = strArr(6).toDouble
      val m12 = month_1 + month_2
      val m34 = month_3 + month_4
      val m56 = month_5 + month_6
      val trend_num = trend(delta, m12, m34, m56)
      user_id + "|" + trend_num
    })
    (data_simplified, topn_class_ratio_rdd, u_trend, user_class)
  }

  def get_u_distinct_classes(data_simplified:
                             RDD[(String, String, String, String, String, String, String)],
                             user_class: RDD[(String, String)]) = {

    //整个计算数据集的所有不重复类别数
    val total_class_count = user_class.map { case (user_id, dptno) => dptno }.distinct().count()

    val u_distinct_classes = user_class.distinct()
      .map { case (user_id, dptno) => (user_id, 1) }
      .reduceByKey(_ + _)
      .map { case (user_id, user_distinct_class_count) =>
        user_id + "|" + user_distinct_class_count + "|" +
          user_distinct_class_count.toDouble / total_class_count
      }
    u_distinct_classes
  }

  //该用户该商品类别的购物数
  def get_user_class_count(user_class: RDD[(String, String)]) = {
    user_class.map { case (user_id, dptno) => ((user_id, dptno), 1) }.reduceByKey(_ + _)
  }

  //只取畅销商品类别前10种，并按照class_ratio的从大到小排序
  def get_u_class_ratio(data_simplified: RDD[(String, String, String, String, String, String, String)],
                        user_class_count: RDD[((String, String), Int)],
                        topn_class_rdd: RDD[(String, Long)],
                        topn_class_ratio: Int) = {
    //该用户的购物数
    val user_total_count = data_simplified.map {
      case (uid, sldat, user_id, item_id, dptno, qty, amt) => (user_id, 1)
    }.reduceByKey(_ + _)

    val u_class_ratio = user_class_count
      .map { case ((user_id, dptno), dptno_count) => (user_id, (dptno, dptno_count)) }
      .join(user_total_count)
      .map {
        case (user_id, ((dptno, dptno_count), u_count)) =>
          (dptno, (user_id, dptno_count.toDouble / u_count))
      }.join(topn_class_rdd)
      .map { case (dptno, ((user_id, u_dptno_ratio), index)) => (user_id, (index, u_dptno_ratio)) }
      .groupByKey()
      .map(x => {
        val user_id = x._1
        val values = x._2.toArray
        val dptno_ratio_map = Map[Long, Double]()
        for (i <- 0 until topn_class_ratio) {
          dptno_ratio_map.put(i, 0.0)
        }
        values.map(x => dptno_ratio_map.put(x._1, x._2))
        val dptno_ratio_str = dptno_ratio_map.toList.sortBy(_._1).map(_._2).mkString("|")
        user_id + "|" + dptno_ratio_str
      })
    u_class_ratio
  }

  def get_u_classes_top5(user_class_count: RDD[((String, String), Int)],
                         topn_class: Int) = {
    val u_classes_top5 = user_class_count.map { case ((user_id, dptno), u_class_count) =>
      (user_id, (u_class_count, dptno))
    }
      .groupByKey()
      .map(x => {
        val user_id = x._1
        val class_count_array = x._2.toArray
        //按照类别数从大到小排序，并给一个索引值
        val sort_index = class_count_array.sortWith((x, y) => (x._1 > y._1)).zipWithIndex
        (user_id, sort_index)
      }).flatMap(x => {
      val user_id = x._1
      val sort_index = x._2
      sort_index.map {
        case ((u_class_count, dptno), index) => (user_id, dptno, u_class_count, index + 1)
      }
    }).filter(x => x._4 <= topn_class)
      .map {
        case (user_id, dptno, u_class_count, index) => user_id + "|" + dptno + "|" + u_class_count
      }

    u_classes_top5
  }

  //  三个值：1, 0, -1 分别表示：上升，平稳，下降
  def trend(delta: Double, m12: Double, m34: Double, m56: Double) = {
    var trend_num = 0
    if ((m12 > m34 && m34 > m56) || (m12 + m34) > (m34 + m56) * (1 + delta)) {
      trend_num = 1
    } else if ((m12 < m34 && m34 < m56) || (m12 + m34) < (m34 + m56) * (1 - delta)) {
      trend_num = -1
    }
    trend_num
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_5_26to27") {
      opt[String]("data_simplified_input_path")
        .action((x, c) => c.copy(data_simplified_input_path = x))
      opt[String]("class_top10_input_path")
        .action((x, c) => c.copy(class_top10_input_path = x))
      opt[String]("user_frequency_month_input_path")
        .action((x, c) => c.copy(user_frequency_month_input_path = x))
      opt[Int]("topn_class")
        .action((x, c) => c.copy(topn_class = x))
      opt[Double]("delta")
        .action((x, c) => c.copy(delta = x))
      opt[Int]("topn_class_ratio")
        .action((x, c) => c.copy(topn_class_ratio = x))
      opt[String]("u_distinct_classes_output_path")
        .action((x, c) => c.copy(u_distinct_classes_output_path = x))
      opt[String]("u_class_ratio_output_path")
        .action((x, c) => c.copy(u_class_ratio_output_path = x))
      opt[String]("u_classes_top5_output_path")
        .action((x, c) => c.copy(u_classes_top5_output_path = x))
      opt[String]("u_trend_output_path")
        .action((x, c) => c.copy(u_trend_output_path = x))
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
                     class_top10_input_path: String = "data/step_3/class_top10",
                     user_frequency_month_input_path: String = "data/step_4/user_frequency_month",
                     topn_class: Int = 5,
                     topn_class_ratio: Int = 10,
                     delta: Double = 0.1,
                     u_distinct_classes_output_path: String = "data/step_5/u_distinct_classes",
                     u_class_ratio_output_path: String = "data/step_5/u_class_ratio",
                     u_classes_top5_output_path: String = "data/step_5/u_classes_top5",
                     u_trend_output_path: String = "data/step_5/u_trend"
                   )

}


