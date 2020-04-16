package com.xiaoi.feature

import com.xiaoi.common.{DateUtil, HadoopOpsUtil, StatsUtil, StrUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/**
  * Written by josh in 2018/7/2
  * order特征计算
  */
object order extends Serializable {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val O_RECENCY_N5_ITEM_LIST = List(1, 2, 3, 4, 5)
  val O_REORDERED_IN_N_DAY_LIST = List(1, 3, 7, 15, 30, 60)
  val PRIOR_W = List(1, 2, 3, 4, 5)

  def run(params: Params): Unit = {


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("order feature")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext

    logger.info("step_5 order features生成......")
    HadoopOpsUtil.removeDir(params.o_recency_n5_item_output_path,
      params.o_recency_n5_item_output_path)
    HadoopOpsUtil.removeDir(params.o_recency_n5_reorder_ratio_output_path,
      params.o_recency_n5_reorder_ratio_output_path)
    HadoopOpsUtil.removeDir(params.o_recency_n1_days_output_path,
      params.o_recency_n1_days_output_path)
    HadoopOpsUtil.removeDir(params.o_reordered_in_60_day_output_path,
      params.o_reordered_in_60_day_output_path)
    HadoopOpsUtil.removeDir(params.o_u_days_since_prior_order_output_path,
      params.o_u_days_since_prior_order_output_path)
    //step_5_25
    HadoopOpsUtil.removeDir(params.u_days_between_orders_sta_output_path,
      params.u_days_between_orders_sta_output_path)
    HadoopOpsUtil.removeDir(params.u_fresh_orders_ratio_output_path,
      params.u_fresh_orders_ratio_output_path)
    HadoopOpsUtil.removeDir(params.u_reorders_ratio_output_path,
      params.u_reorders_ratio_output_path)
    HadoopOpsUtil.removeDir(params.u_reorders_duplicate_items_ratio_output_path,
      params.u_reorders_duplicate_items_ratio_output_path)

    // 准备数据  data_14cols_1
    val data_14cols_1 = get_data_14cols_1(params.data_14cols_1_input_path, sc)
    data_14cols_1.cache()

    logger.info("计算观察点的信息 缺省取最后一天..")
    val obverve_info = get_observe_info(data_14cols_1,
      params.observe_time_enabled, params.observe_time)
    val observe_day = obverve_info._1
    val observe_user_item_uid = obverve_info._2

    logger.info("key为user+item的order特征一起算完...")
    //step_5_11 -- step_5_13
    val order_11to13 = get_order_features_by_ui(data_14cols_1,
      params.observe_time_enabled, observe_day)

    logger.info("o_recency_n5_item")
    // user_id , item_id , n1, n2, n3, n4, n5, r1, r2, r3, r4, r5
    order_11to13._1.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.o_recency_n5_item_output_path)

    logger.info("o_recency_n5_reorder_ratio")
    // user_id, item_id, w1_ratio, w2_ratio, w3_ratio, w4_ratio, w5_ratio （1最近，5最远 ）
    order_11to13._2.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.o_recency_n5_reorder_ratio_output_path)

    logger.info("o_recency_n1")
    //user_id, item_id, days, sessions, reorder_ratio, trend
    order_11to13._3.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.o_recency_n1_days_output_path)

    logger.info("o_reordered_in_60_day")
    //user_id, item_id, d1, d3, d7, d15, d30, d60 d1表示最近1天内的购买数量
    order_11to13._4.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.o_reordered_in_60_day_output_path)

    logger.info("key为user的order特征...")
    val o_days_since_prior = get_order_feature_by_user(data_14cols_1,
      observe_user_item_uid, observe_day)
    // user_id, last_order_time, days
    o_days_since_prior.map(x => x._1 + "|" + x._2)
      .saveAsTextFile(params.o_u_days_since_prior_order_output_path)

    //step_5_25
    logger.info("u_days_between_orders_sta		字段：user_id, min, max, median, avg, std")
    val u_days_between_orders_sta = get_u_days_between_orders(data_14cols_1)
    u_days_between_orders_sta.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.u_days_between_orders_sta_output_path)

    logger.info("u_fresh_orders_ratio			字段：user_id, count, ratio")
    val u_fresh_orders_ratio = get_u_fresh_orders(data_14cols_1)
    u_fresh_orders_ratio.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.u_fresh_orders_ratio_output_path)

    logger.info("u_reorders_ratio				字段：user_id, ratio")
    val u_reorders_ratio = get_u_reorders_ratio(data_14cols_1)
    u_reorders_ratio.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.u_reorders_ratio_output_path)

    logger.info("u_reorders_duplicate_items_ratio	字段：user_id, w1, w2, w3, w4, w5")
    val u_reorders_duplicate_items_ratio =
      get_u_reorders_duplicate_items(data_14cols_1, PRIOR_W)
    u_reorders_duplicate_items_ratio.map(StrUtil.tupleToString(_, "|"))
      .saveAsTextFile(params.u_reorders_duplicate_items_ratio_output_path)

    sc.stop()
  }

  /**
    * 过去(1,2,3,4,5)次购物的重复购买item比例率
    * (过去W次购物的items∩本次购物的items）/本次购物的items
    */
  def get_o_recency_n5_reorder_ratio(item_group: List[(String, (String, List[String]))],
                                     index_flag: Int,
                                     this_item: String,
                                     this_uid: String,
                                     this_time: String,
                                     iter: (String, (String, List[String])),
                                     n1_reorder_ratio: ListBuffer[(String, String, String, String)]) = {
    var count_str: ListBuffer[(Double)] = ListBuffer.empty
    for (w <- PRIOR_W) { //ratio
      val compare_items = item_group.par.drop(index_flag).take(w)
      val now_items = iter._2._2 //一个回合item有可能出现多次，不去重
      val reorder_ratio = if (compare_items.size < 1) 0.0
        else StatsUtil.double_ratio(compare_items
          .map(_._2._2).reduce(_ ++ _).intersect(now_items).size, now_items.size)
      count_str += reorder_ratio
    }
    n1_reorder_ratio += Tuple4(this_item, this_uid, count_str.mkString("|"), this_time)
  }

  /**
    * 最近一次购买该商品到现在的时间差
    * reorder_ratio :
    * 两个order之间的所有商品的重复购买率，分母两个order之间所有购买记录数，分子为分母去重之后数
    */
  def get_o_recency_n1_days(item_group: List[(String, (String, List[String]))],
                            index_flag: Int,
                            this_item: String,
                            this_uid: String,
                            this_time: String,
                            iter: (String, (String, List[String])),
                            recency_n1_days: ListBuffer[(String, String, String, String)]) = {
    //user_id, item_id, days, sessions, reorder_ratio, trend
    var flag = true
    var index_w = 0
    val time_list = item_group.par.filter(x => x._2._2.contains(this_item)).map(_._2._1).toList
    //获得ui对应的所有时间
    val ui_reorder_days_between_orders_avg = if (time_list.size <= 1) 0.0
      else {
        StatsUtil.mean(time_list.sliding(2).toList
          .map(x => DateUtil.dateTimeDiffInDay(x(0), x(1))).map(x => math.abs(x.toDouble)).toArray)
      }

    for (group <- item_group.drop(index_flag)) { //找到上一次购买this_item的回合
      if (group._2._2.contains(this_item) && flag == true) {
        index_w = item_group.indexOf(group)

        val prior_uid = item_group(index_w)
        val prior_time = item_group.par(index_w)._2._1
        val days = DateUtil.dateTimeDiffInDay(prior_time, this_time) //days
        val sessions = index_w + 1 - index_flag //sessions

        val prior_items = prior_uid._2._2
        val prior_to_now_items = if (prior_items.size < 1) iter._2._2
          else prior_items ++ iter._2._2
        val reorder_ratio = StatsUtil.double_ratio(prior_to_now_items.distinct.size, prior_to_now_items.size) //reorder_ratio
        val trend = days - ui_reorder_days_between_orders_avg //tread
        val result_str = StrUtil.tupleToString((days, sessions, reorder_ratio, trend), "|")
        recency_n1_days += Tuple4(this_item, this_uid, result_str, this_time)
        flag = false
      }
    }
  }

  /**
    * 该用户最近5次订单购买该item的次数, ratio
    */
  def get_o_recency_n5_item(item_group: List[(String, (String, List[String]))],
                            index_flag: Int,
                            this_item: String,
                            this_uid: String,
                            this_time: String,
                            iter: (String, (String, List[String])),
                            recency_n5_item: ListBuffer[(String, String, String, String)]) = {
    var count_str: ListBuffer[(Double)] = ListBuffer.empty
    for (w <- PRIOR_W) { //count
      val compare_items = item_group.par.drop(index_flag).take(w)
      val reorder_count = if (compare_items.size < 1) 0.0
      else compare_items.map(_._2._2).reduce(_ ++ _).filter(_ == this_item).size
      count_str += reorder_count
    }
    for (w <- PRIOR_W) { //ratio
      val compare_items = item_group.par.drop(index_flag).take(w)
      val now_items = iter._2._2 //一个回合item有可能出现多次，不去重
      val reorder_ratio = if (compare_items.size < 1) 0.0
      else StatsUtil.double_ratio(compare_items
        .map(_._2._2).reduce(_ ++ _).filter(_ == this_item).size,
        compare_items.map(_._2._2).reduce(_ ++ _).size)
      count_str += reorder_ratio
    }
    recency_n5_item += Tuple4(this_item, this_uid, count_str.mkString("|"), this_time)
  }

  def get_o_reordered_in_n_day(item_group: List[(String, (String, List[String]))],
                               index_flag: Int,
                               this_item: String,
                               this_uid: String,
                               this_time: String,
                               iter: (String, (String, List[String])),
                               reordered_in_60_day: ListBuffer[(String, String, String, String)],
                               before_days: List[(Int)]) = {
    //o_reordered_in_60_day    	字段： user_id, item_id, d1, d3, d7, d15, d30, d60
    var count_str: ListBuffer[(Double)] = ListBuffer.empty
    for (days <- before_days) {
      val item_list = item_group.drop(index_flag).map(x => (x._2._1, x._2._2)) //time, item
        .filter(x => x._1 < this_time && x._1 >= DateUtil.getTimeBefore(this_time, days))
        .map(_._2) //items
      val buy_count = if (item_list.size < 1) 0.0
      else {
        item_list.reduce(_ ++ _).filter(_ == this_item).size
      }
      count_str += buy_count
    }
    reordered_in_60_day += Tuple4(this_item, this_uid, count_str.mkString("|"), this_time)
  }

  def get_order_features_by_ui(data_14cols_1:
                               RDD[(String, String, String, String, String, String, String)],
                               observe_time_enabled: Boolean,
                               observe_time: String) = {
    val observe_day = if (observe_time_enabled) observe_time else {
      data_14cols_1.map(_._2)
        .max()
        .slice(0, 10)
    }
    //(过去W次购物的items∩本次购物的items）/本次购物的items
    val order_result_list = data_14cols_1.map {
      case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
      (user_id, (uid, item_id, sldat))
    }.groupByKey()
      .mapValues(uid_item_time => {
        val item_group = uid_item_time
          .groupBy(_._1).mapValues(x => {
          val items = x.map(_._2)
          val time = x.map(_._3).min
          (time, items)
        }).mapValues(x => (x._1, x._2.toList)).toList
          .sortBy(_._2._1).reverse //List(uid, (time, Set(item)))
        //最近5次 特征
        var recency_n5_item = ListBuffer[(String, String, String, String)]()
        var n1_reorder_ratio = ListBuffer[(String, String, String, String)]()
        //最近1次 特征
        var recency_n1_days = ListBuffer[(String, String, String, String)]()
        //最近60天
        var reordered_in_60_day = ListBuffer[(String, String, String, String)]()
        var index_flag = 1
        for (iter <- item_group) { //for each uid
          val this_time = iter._2._1
          val this_uid = iter._1
          for (this_item <- iter._2._2) { //for each item in one uid

            //o_recency_n5_item
            get_o_recency_n5_item(item_group,
              index_flag, this_item, this_uid, this_time, iter, recency_n5_item)

            //o_recency_n5_reorder_ratio
            get_o_recency_n5_reorder_ratio(item_group,
              index_flag, this_item, this_uid, this_time, iter, n1_reorder_ratio)

            //o_recency_n1_days
            get_o_recency_n1_days(item_group,
              index_flag, this_item, this_uid, this_time, iter, recency_n1_days)

            //o_reordered_in_60_day
            get_o_reordered_in_n_day(item_group,
              index_flag, this_item, this_uid, this_time, iter, reordered_in_60_day,
              O_REORDERED_IN_N_DAY_LIST)
          }
          index_flag += 1
        }
        val o_recency_n5_item = filter_observe_time(recency_n5_item, observe_day)
        val o_recency_n5_reorder_ratio = filter_observe_time(n1_reorder_ratio, observe_day)
        val o_recency_n1_days = filter_observe_time(recency_n1_days, observe_day)
        //默认取一天，可能当天会有重复 
        val o_reordered_in_60_day = filter_observe_time(reordered_in_60_day, observe_day)

        (o_recency_n5_item, o_recency_n5_reorder_ratio, o_recency_n1_days, o_reordered_in_60_day)
      }).cache()
    (order_result_list.map(x => (x._1, x._2._1)).flatMapValues(x => x),
      order_result_list.map(x => (x._1, x._2._2)).flatMapValues(x => x),
      order_result_list.map(x => (x._1, x._2._3)).flatMapValues(x => x),
      order_result_list.map(x => (x._1, x._2._4)).flatMapValues(x => x)
    )
  }

  // 通过观察点日期过滤数据量
  def filter_observe_time(duplicates: ListBuffer[(String, String, String, String)],
                          observe_day: String) = {
    val observe_time_0 = observe_day + " 00:00:00"
    duplicates.filter { case (item, uid, result, sldat) =>
      observe_time_0 <= sldat && sldat <= DateUtil.getTimeBefore(observe_time_0, -1)
    }
    .map(x => (x._1, x._3)) //value只取item和计算值
    .map(StrUtil.tupleToString(_, "|"))
  }

  def get_u_reorders_duplicate_items(data_14cols_1:
                                     RDD[(String, String, String, String, String, String, String)],
                                     PRIOR_W: List[(Int)]) = {
    //(过去W次购物的items∩本次购物的items）/本次购物的items
    data_14cols_1.map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
      (user_id, (uid, item_id, sldat))
    }
      .groupByKey()
      .mapValues(uid_item_time => {
        val item_group = uid_item_time.groupBy(_._1).mapValues(x => {
          val items = x.map(_._2)
          val time = x.map(_._3).min
          (time, items)
        }).map(x => (x._2._1, x._2._2.toSet)).toList
          .sortBy(_._1).reverse.map(_._2) //List(Set(item))
        var duplicates: ListBuffer[(Double)] = ListBuffer.empty
        for (i <- PRIOR_W) {
          if (item_group.size <= 1) {
            duplicates += 0.0
          } else {
            val now_items = item_group(0)
            val reorder_ratio = StatsUtil.double_ratio((now_items
              intersect item_group.drop(1).take(i).reduce(_ ++ _)).size, now_items.size)
            duplicates += reorder_ratio
          }
        }
        duplicates.mkString("|")
      })
  }

  def get_u_reorders_ratio(data_14cols_1: RDD[(String, String, String, String, String, String, String)]) = {
    //1-不重复商品名称数/交易记录数
    data_14cols_1.map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
      (user_id, (uid, item_id))
    }
    .groupByKey().mapValues(uid_item => {
      val log_count = uid_item.toList.size
      val items = uid_item
        .groupBy(_._1).mapValues(_.map(_._2).toList.distinct) //uid,items
        .map(_._2).flatMap(x => x).toList
      1.0 - StatsUtil.double_ratio(
        items.map((_, 1)).groupBy(_._1).mapValues(_.size).filter(_._2 <= 1).size, log_count)
    })
  }

  def get_u_fresh_orders(data_14cols_1: RDD[(String, String, String, String, String, String, String)]) = {
    val fresh_orders = data_14cols_1.map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
      (user_id, (uid, item_id, sldat))
    }
      .groupByKey()
      .mapValues(uid_item_time => {
        val uid_count = uid_item_time.map(_._1).toList.distinct.size
        val item_group = uid_item_time.groupBy(_._1).mapValues(x => {
          val items = x.map(_._2)
          val time = x.map(_._3).min
          (time, items)
        }).map(x => (x._2._1, x._2._2.toSet)).toList
          .sortBy(_._1).map(_._2) //List(Set(item))
        val fresh_count = if (item_group.size <= 1) 0
        else {
          var fresh_uid: Long = 0
          var list_iterater = 1
          for (value <- item_group.drop(1)) {
            if ((value -- item_group.take(list_iterater).reduce(_ ++ _)).size >= 1) {
              fresh_uid += 1
            }
            list_iterater += 1
          }
          fresh_uid
        }
        StatsUtil.double_ratio(fresh_count, uid_count)
      })
    fresh_orders
  }

  def get_u_days_between_orders(data_14cols_1: 
      RDD[(String, String, String, String, String, String, String)]) = {
    val days_sta = data_14cols_1.map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
      ((user_id, uid), sldat)
    }
      .groupByKey().mapValues(_.toList.min)
      .map(x => (x._1._1, x._2)) //(user, time)
      .groupByKey().mapValues(dateList => {
      val days_array = if (dateList.size == 1) Array(0.0) else {
        dateList
          .filter(_.size > 1)
          .toList.sortWith(_ < _).sliding(2)
          .map(tuple => DateUtil.dateTimeDiffInDay(tuple(0), tuple(1)))
          .map(_.toDouble).toArray
      }
      StatsUtil.sta_Count(days_array)
    })
    days_sta
  }


  //  u_days_between_orders_sta		字段：user_id, min, max, median, avg, std
  //  u_fresh_orders_ratio			字段：user_id, count, ratio
  //总的新颖购物次数/ 交易回合数 （衡量用户是否喜欢购买新物品
  //  u_reorders_ratio				字段：user_id, ratio
  //  u_reorders_duplicate_items_ratio	字段：user_id, w1, w2, w3, w4, w5
  def get_observe_info(data_14cols_1: RDD[(String, String, String, String, String, String, String)],
                       observe_time_enabled: Boolean,
                       observe_time: String) = {
    val observe_day = if (observe_time_enabled) observe_time else {
      data_14cols_1.map(_._2)
        .max()
        .slice(0, 10)
    }
    val observe_time_0 = observe_day + " 00:00:00"
    val observe_user_item_uid = data_14cols_1
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        ((user_id, uid), (item_id, sldat))
      }
      .groupByKey()
      .mapValues(x => {
        val min_time = x.map(_._2).min
        x.map(u_s => (u_s._1, min_time))
      })
      .flatMapValues(x => x)
      .filter { case ((user_id, uid), (item_id, sldat)) =>
        observe_time_0 <= sldat && sldat <= DateUtil.getTimeBefore(observe_time_0, -1)
      }.map { case ((user_id, uid), (item_id, sldat)) => ((user_id, item_id), (uid, sldat)) }
    (observe_day, observe_user_item_uid)
  }

  def get_data_14cols_1(step_2_2: String, sc: SparkContext):
      RDD[(String, String, String, String, String, String, String)] = {
    val data_14cols_1 = sc.textFile(step_2_2).map { line =>
      val tokens = line.split("\\|", -1)
      //UID,SLDAT,user_id,PRODNO,item_id,DPTNO,DPTNAME,BNDNO,BNDNAME,QTY,AMT
      val uid = tokens(0)
      val sldat = tokens(1)
      val user_id = tokens(2)
      val item_id = tokens(4)
      val dptno = tokens(5)
      val qty = tokens(9)
      val amt = tokens(10)
      (uid, sldat, user_id, item_id, dptno, qty, amt)
    }
    data_14cols_1
  }


  /**
    * Order 特征 join的key为(user, uid)
    **/
  def get_order_feature_by_user(data_14cols_1: RDD[(String, String, String, String, String, String, String)],
                                observe_user_item_uid: RDD[((String, String), (String, String))],
                                observe_day: String): RDD[(String, String)] = {

    // step_5_14    最近一次购物到现在时间差
    // o_u_days_since_prior_order   字段： user_id,  last_order_time, days  最近一次购物时间， 距现在天数
    // o_days_since_prior_order_this 最近一次购买该商品到现在的时间差  join的key为(user, uid)
    val user_oneTime = data_14cols_1
      .map(x => ((x._3, x._1), x._2))
      .groupByKey()
      .mapValues(x => x.take(1))
      .flatMapValues(x => x)
      .map(x => (x._1._1, (x._1._2, x._2)))

    val observe_oneTime = observe_user_item_uid
      .map { case (((user_id, item_id), (uid, sldat))) => (user_id, (uid, sldat)) }

    val o_days_since_prior_order = observe_oneTime
      .join(user_oneTime
        .map(x => (x._1, x._2._2))
        .groupByKey()
        .mapValues(x => x.toList.sortBy(x => x)))
      .map { case (user, ((uid, time), list_time)) => ((user, uid), (time, list_time)) }
      .mapValues(x => {
        val item_listItem = x
        val time_now = item_listItem._1
        val list_items = item_listItem._2
        val diff_time = if (list_items.size == 1) 0
          else if (list_items.indexOf(time_now) == 0) 0
          else DateUtil.dateTimeDiffInDay(list_items(list_items.indexOf(time_now) - 1), time_now)
        (time_now, observe_day, diff_time.toDouble)
      }).map { case ((user, uid), (time, observe_time, diff_time)) =>
      (user, (observe_time + "|" + diff_time))
    }
    o_days_since_prior_order
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("order_features") {
      opt[String]("data_14cols_1_input_path")
        .action((x, c) => c.copy(data_14cols_1_input_path = x))
      opt[Boolean]("observe_time_enabled")
        .action((x, c) => c.copy(observe_time_enabled = x))
      opt[String]("observe_time")
        .action((x, c) => c.copy(observe_time = x))
      opt[String]("o_recency_n5_item_output_path")
        .action((x, c) => c.copy(o_recency_n5_item_output_path = x))
      opt[String]("o_recency_n5_reorder_ratio_output_path")
        .action((x, c) => c.copy(o_recency_n5_reorder_ratio_output_path = x))
      opt[String]("o_recency_n1_days_output_path")
        .action((x, c) => c.copy(o_recency_n1_days_output_path = x))
      opt[String]("o_reordered_in_60_day_output_path")
        .action((x, c) => c.copy(o_reordered_in_60_day_output_path = x))
      opt[String]("o_u_days_since_prior_order_output_path")
        .action((x, c) => c.copy(o_u_days_since_prior_order_output_path = x))

      opt[String]("u_days_between_orders_sta_output_path")
        .action((x, c) => c.copy(u_days_between_orders_sta_output_path = x))
      opt[String]("u_fresh_orders_ratio_output_path")
        .action((x, c) => c.copy(u_fresh_orders_ratio_output_path = x))
      opt[String]("u_reorders_ratio_output_path")
        .action((x, c) => c.copy(u_reorders_ratio_output_path = x))
      opt[String]("u_reorders_duplicate_items_ratio_output_path")
        .action((x, c) => c.copy(u_reorders_duplicate_items_ratio_output_path = x))
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
                     data_14cols_1_input_path: String = "data/step_3/data_14cols_1",
                     observe_time_enabled: Boolean = false,
                     observe_time: String = "",
                     o_recency_n5_item_output_path: String = "",
                     o_recency_n5_reorder_ratio_output_path: String = "",
                     o_recency_n1_days_output_path: String = "",
                     o_reordered_in_60_day_output_path: String = "",
                     o_u_days_since_prior_order_output_path: String = "",

                     u_days_between_orders_sta_output_path: String = "",
                     u_fresh_orders_ratio_output_path: String = "",
                     u_reorders_ratio_output_path: String = "",
                     u_reorders_duplicate_items_ratio_output_path: String = ""
                   )

}
