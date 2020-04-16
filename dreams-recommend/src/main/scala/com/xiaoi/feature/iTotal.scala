package com.xiaoi.feature

import com.xiaoi.common.{DateUtil, HadoopOpsUtil, StatsUtil, StrUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/**
 * Written by josh in 2018/7/2
 * 对应step_5 item_31to37   item特征， 包括购物篮、时间相关、购买趋势、价格等特征
 * 有比较复杂的strike特征
 */
object iTotal {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val I_TOP100_INDEX = 0
  val I_PRICE_STA_INDEX = 0
  val I_AVG_PRICE_RANK_PERCENT_INDEX =1
  val I_BASKET_SIZE_STA_INDEX = 0
  val I_ONLY_ME_RATIO_INDEX = 1
  val I_AVG_BASKET_RATIO_INDEX = 2
  val I_BASKET_CLASSES_COUNT_STA_INDEX=3
  val I_BASKET_SAME_CLASS_COUNT_STA_INDEX=4
  val I_TIME_DOW_RATIO_INDEX=0
  val I_TIME_HOUR_RATIO_INDEX=1
  val I_TIME_MONTH_RATIO_INDEX=2
  val I_TIME_SEASON_RATIO_INDEX=3
  val I_TIME_WORKDAY_RATIO_INDEX=4
  val I_ORDER_DAYS_RATIO_INDEX=5
  val I_3ORDERS_DAYS_RATIO_INDEX=6
  val I_REORDER_INFO_INDEX=0
  val I_REORDER_RATIO_TOP_N_INDEX=1
  val I_STRIKE_INDEX=0
  val I_TREND_INDEX=0

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("step_5 item_31to37 or iTotal")
    val sc = new SparkContext(conf)
    logger.info("item 特征计算 数据保存路径去存... ")
    val rootPath = params.root_output_path
    val step_5_31 = savePathSplit(rootPath, params.i_top100_output_path, I_TOP100_INDEX)
    val step_5_32_1 = savePathSplit(rootPath, params.i_price_output_path, I_PRICE_STA_INDEX)
    val step_5_32_2 = savePathSplit(rootPath, params.i_price_output_path, I_AVG_PRICE_RANK_PERCENT_INDEX)
    val step_5_33_1 = savePathSplit(rootPath, params.i_basket_output_path, I_BASKET_SIZE_STA_INDEX)
    val step_5_33_2 = savePathSplit(rootPath, params.i_basket_output_path, I_ONLY_ME_RATIO_INDEX)
    val step_5_33_3 = savePathSplit(rootPath, params.i_basket_output_path, I_AVG_BASKET_RATIO_INDEX)
    val step_5_33_4 = savePathSplit(rootPath, params.i_basket_output_path, I_BASKET_CLASSES_COUNT_STA_INDEX)
    val step_5_33_5 = savePathSplit(rootPath, params.i_basket_output_path, I_BASKET_SAME_CLASS_COUNT_STA_INDEX)
    val step_5_34_1 = savePathSplit(rootPath, params.i_time_output_path, I_TIME_DOW_RATIO_INDEX)
    val step_5_34_2 = savePathSplit(rootPath, params.i_time_output_path, I_TIME_HOUR_RATIO_INDEX)
    val step_5_34_3 = savePathSplit(rootPath, params.i_time_output_path, I_TIME_MONTH_RATIO_INDEX)
    val step_5_34_4 = savePathSplit(rootPath, params.i_time_output_path, I_TIME_SEASON_RATIO_INDEX)
    val step_5_34_5 = savePathSplit(rootPath, params.i_time_output_path, I_TIME_WORKDAY_RATIO_INDEX)
    val step_5_34_6 = savePathSplit(rootPath, params.i_time_output_path, I_ORDER_DAYS_RATIO_INDEX)
    val step_5_34_7 = savePathSplit(rootPath, params.i_time_output_path, I_3ORDERS_DAYS_RATIO_INDEX)
    val step_5_35_1 = savePathSplit(rootPath, params.i_reorder_output_path, I_REORDER_INFO_INDEX)
    val step_5_35_2 = savePathSplit(rootPath, params.i_reorder_output_path, I_REORDER_RATIO_TOP_N_INDEX)
    val step_5_36 = savePathSplit(rootPath, params.i_strike_output_path, I_STRIKE_INDEX)
    val step_5_37 = savePathSplit(rootPath, params.i_trend_output_path, I_TREND_INDEX)

    logger.info(s"step_5_31 i_top100 saving: ${step_5_31}")
    //step_5_31
    /**
     * item_id
     * count
     * rank
     */
    val i_top100 = get_item_top100(params.item_total_input_path, step_5_31, sc)
    i_top100.map(StrUtil.tupleToString(_,"|")).repartition(1)
      .saveAsTextFile(step_5_31)

    logger.info(s"读取里程碑数据 data_simplified...")
    // 准备数据 data_simplified
    val data_simplified = get_data_simplified(params.data_simplified_input_path, sc)
    data_simplified.cache()
    //去重后的所有商品总数
    val total_pluname_dis = data_simplified.map(x => x._4).distinct().count()
    //所有商品总的交易回合
    val total_uid = data_simplified.map(_._1).distinct().count()

    logger.info(s"step_5_32 价格信息...")
    //step_5_32
    /**
     * i_price_sta :  item_id, min, max, median, avg, std
     * i_avg_price_rank_percent:  item_id, rank_percent
     *
     */
    val price_info_rdd = get_price_info(data_simplified, total_pluname_dis)
    price_info_rdd._1.repartition(1).saveAsTextFile(step_5_32_1)
    price_info_rdd._2.repartition(1).saveAsTextFile(step_5_32_2)

    logger.info(s"step_5_33 购物篮特征..")
    val basket_info_rdd = get_basket_info(data_simplified, step_5_33_1,total_uid)

    /**
     * i_basket_size_sta :  item_id, min, max, median, avg, std
     */
    logger.info(s"step_5_33  i_basket_size_sta save...${step_5_33_1}")
    basket_info_rdd._1.repartition(1).saveAsTextFile(step_5_33_1)

    /**
     * i_only_me_ration:  item_id, ratio
     *
     */
    logger.info(s"i_only_me_ratio save: ${step_5_33_2}")
    basket_info_rdd._2.repartition(1).saveAsTextFile(step_5_33_2)

    /**
     * i_avg_basket_ratio:  item_id, ratio
     */
    logger.info(s"i_avg_basket_ratio save: ${step_5_33_3}")
    basket_info_rdd._3.repartition(1).saveAsTextFile(step_5_33_3)

    /**
     * i_basket_classes_count_sta : item_id, min, max, median, avg, std
     */
    logger.info(s"i_basket_classes_count_sta write: ${step_5_33_4}")
    basket_info_rdd._4.repartition(1).saveAsTextFile(step_5_33_4)

    /**
     * i_basket_same_class_count_sta:  item_id, min, max, median, avg, std
     */
    logger.info(s"i_basket_same_class_count_sta write: ${step_5_33_5}")
    basket_info_rdd._5.repartition(1).saveAsTextFile(step_5_33_5)

    //step_5_34
    logger.info("i_time_dow_ratio 字段：item_id, dow1, dow2, dow3, dow4, dow5 , dow6, dow7")
    val i_time_dow_ratio = temporal_dist(sc, rootPath, params.ui_time_dist_input_path, 0, 0, 7-1)
    i_time_dow_ratio.repartition(1).saveAsTextFile(step_5_34_1)

    logger.info("i_time_hour_ratio	字段：  item_id, h6, h7, ......, h23")
    val i_time_hour_ratio = temporal_dist(sc, rootPath, params.ui_time_dist_input_path, 1, 0, 23-6)
    i_time_hour_ratio.repartition(1).saveAsTextFile(step_5_34_2)

    logger.info("i_time_month_ratio 字段：  item_id, m1, ......., m12")
    val i_time_month_ratio = temporal_dist(sc, rootPath, params.ui_time_dist_input_path, 2, 0, 12-1)
    i_time_month_ratio.repartition(1).saveAsTextFile(step_5_34_3)

    logger.info("i_time_season_ratio			字段：  item_id, s1, s2, s3, s4")
    val i_time_season_ratio = temporal_dist(sc,rootPath, params.ui_time_dist_input_path, 3, 0, 4-1)
    i_time_season_ratio.repartition(1).saveAsTextFile(step_5_34_4)

    logger.info("step_5_34  i 时间特征... ")
    val item_time_info = get_item_time_info(data_simplified, params.ui_time_dist_input_path,
      rootPath, sc)
    item_time_info._1.repartition(1).saveAsTextFile(step_5_34_5)
    item_time_info._2.repartition(1).saveAsTextFile(step_5_34_6)
    item_time_info._3.repartition(1).saveAsTextFile(step_5_34_7)

    logger.info(s"reading user_items_order_list: ${params.user_baskets_has_item_list_input_path}")
    val user_items_order_list = get_user_baskets_item(sc,
      params.user_baskets_has_item_list_input_path)
    user_items_order_list.cache()


    logger.info("step_5_35  i reorder特征 ..")
    /**
     * item_id
     * count
     * ratio
     * user_ratio
     * days_since_prior_avg
     */
    val i_reorder_info = get_item_order_features(user_items_order_list,
      params.data_14cols_1_input_path, params.reorder_top_Num, sc)
    logger.info(s"i_reorder_info write ${step_5_35_1}")
    i_reorder_info._1.repartition(1).saveAsTextFile(step_5_35_1)

    /**
     * item_id
     * ratio
     * days_since_prior_avg
     */
    logger.info(s" i_reorder_ratio_top_${params.reorder_top_Num} write ${step_5_35_2}")
    i_reorder_info._2.repartition(1).saveAsTextFile(step_5_35_2)


    logger.info("step_5_36 i stike 特征 ")
    val i_strike_rdd = get_i_strike(user_items_order_list,sc)
    i_strike_rdd.repartition(1).saveAsTextFile(step_5_36)

    logger.info("step_5_37 i trend 特征 ")
    val i_trend_rdd = get_i_trend(params.item_frequency_month_input_path,
      params.trend_delta, sc)
    i_trend_rdd.repartition(1).saveAsTextFile(step_5_37)

    sc.stop()
  }

  def get_user_baskets_item(sc: SparkContext, user_baskets_item: String):
  RDD[(String, String, Array[String])] = {
    sc.textFile(user_baskets_item)
      .map(x => x.split("\\|")).map(s => (s(0), s(1), s.drop(2)))
  }

  // step_5_31
  //item基本信息   已经在  step_3/item_total
  // i_top100        	字段： item_id，count, rank      最受欢迎的前100个item
  // 输入：  step_3/itme_total
  def get_item_top100(step_3_31: String, step_5_31: String, sc: SparkContext) = {
    val item_total = sc.textFile(step_3_31).map { line =>
      val tokens = line.split("\\|", -1)
      //item_id, first_order_time, log_count, log_ratio, session_count, session_ratio,
      // user_count, user_ratio,  money
      val item_id = tokens(0)
      val log_count = tokens(2)
      (item_id, log_count)
    }
    val i_top100 = item_total
      .sortBy(x=>x._2, false)
      .zipWithIndex()
      .map{ case ((item_id, count), item_rank) => (item_id, count, item_rank+1) }
    i_top100
  }

  def get_data_simplified(step_3_data_simplified: String, sc: SparkContext):
  RDD[(String, String, String, String, String, String, String)] = {
    val data_simplified = sc.textFile(step_3_data_simplified).map { line =>
      val tokens = line.split("\\|", -1)
      //uid, sldat, user_id，item_id，dptno, qty, amt
      val uid = tokens(0)
      val sldat = tokens(1)
      val user_id = tokens(2)
      val item_id = tokens(3)
      val dptno = tokens(4)
      val qty = tokens(5)
      val amt = tokens(6)
      (uid, sldat, user_id, item_id, dptno, qty, amt)
    }
    data_simplified
  }

  //step_5_32    价格信息
  //i_price_sta 字段： item_id, min, max, median, avg, std
  def get_price_info(data_simplified:
                     RDD[(String, String, String, String, String, String, String)],
                     total_pluname_dis: Long) = {

    val i_price_sta = data_simplified.filter(!_._6.equals("0")).map{
      case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        (item_id, StatsUtil.double_ratio(amt.toDouble, qty.toDouble))
    }.groupByKey()
      .mapValues(x => StatsUtil.sta_Count(x.toArray))

    //i_avg_price_rank_percent:	本商品的平均价格在所有商品价格排序中的百分比 字段： item_id, rank_percent
    //所有商品的价格平均值由大到小排序
    val price_pluname_sorted = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        (item_id, (qty, amt))}
      .groupByKey()
      .mapValues(amtQty => {
        val price = StatsUtil.mean(amtQty.map(x => StatsUtil.double_ratio(x._2, x._1)).toArray)
        price
      }).sortBy(x => x._2, false)
      .zipWithIndex()
    val i_avg_price_rank_percent = price_pluname_sorted
      .map{case ((item_id, avg_price), rank) =>
        (item_id, StatsUtil.double_ratio(rank+1, total_pluname_dis))}

    (i_price_sta.map(StrUtil.tupleToString(_, "|")), i_avg_price_rank_percent.map(StrUtil.tupleToString(_, "|")))
  }

  //step_5_33   购物篮特征
  //i_basket_size_sta, 字段：item_id, min, max, median, avg, std,
  def get_basket_info(data_simplified:
                      RDD[(String, String, String, String, String, String, String)],
                      step_5_33_1: String,
                      total_uid: Long): (RDD[(String)], RDD[(String)],
    RDD[(String)], RDD[(String)],RDD[(String)]) = {

    val i_basket_size_sta = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        (uid, item_id)}.groupByKey().mapValues(items => {
      val uid_size = items.size
      items.map(x=> (x, uid_size))
    }).flatMapValues(x =>x)
      .map{case (uid, (item, size)) =>(item, size)}
      .groupByKey()
      .mapValues(sizeList=> StatsUtil.sta_Count(sizeList.map(_.toDouble).toArray))

    //i_only_me_ratio	i_only_me_ratio 独领风骚占比 字段：item_id, ratio
    //哪些商品是单独买的 item_id, count
    val only_one_items = data_simplified
      .map(x => (x._1, x._4))
      .groupByKey()
      .filter(_._2.size == 1)
      .map(x => (x._2.toList(0), 1))
      .reduceByKey(_ + _)

    val i_only_me_ratio = data_simplified
      .map(x => (x._4, total_uid))
      .distinct()
      .leftOuterJoin(only_one_items)
      .map(x => (x._1, StatsUtil.double_ratio(x._2._2.getOrElse(0), x._2._1)))

    //    i_avg_basket_ratio   	字段： item_id, ratio
    val i_avg_basket_ratio = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) => (uid, item_id)}
      .groupByKey()
      .mapValues(items =>
      {items.map(item=> (item, StatsUtil.double_ratio(1, items.toList.distinct.size)))
      }).flatMapValues(x=>x)
      .map{case (uid, (item, ratio)) => (item, ratio)}
      .groupByKey()
      .mapValues(ratioList => StatsUtil.mean(ratioList.toArray))

    //i_basket_classes_count_sta       	字段： item_id, min, max, median, avg, std
    val i_basket_classes_count_sta = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        (uid, (item_id, dptno))}
      .groupByKey()
      .mapValues(items_dpt => {
        val dptNum = items_dpt.map(item => (item._1, items_dpt.map(_._2).toList.distinct.size))
        dptNum
      }).flatMapValues(x=>x)
      .map{case (uid, (item, dptNum)) => (item, dptNum)}
      .groupByKey()
      .mapValues(ratioList=> StatsUtil.sta_Count(ratioList.map(_.toDouble).toArray))

    //    i_basket_same_class_count_sta
    // 字段： item_id, min, max, median, avg, std  同类别的商品的独立商品数统计值
    val i_basket_same_class_count_sta = data_simplified
      .map { case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        (uid, (item_id, dptno))}
      .groupByKey().mapValues(items_dpt => {
      val same_class_count = items_dpt
        .map{case (item_id, dpt) => (dpt, item_id)}
        .groupBy(_._1)
        .map(m=> m._2.map(i=>(i._2,m._2.size)))
        .flatMap(x=>x)
      same_class_count
    }).flatMapValues(x=>x)
      .map{case (uid, (item, same_class_count)) =>
        (item, same_class_count)}
      .groupByKey()
      .mapValues(ratioList => StatsUtil.sta_Count(ratioList.map(_.toDouble).toArray))

    (i_basket_size_sta.map(x=>x._1+"|"+x._2),
      i_only_me_ratio.map(x=>x._1+"|"+x._2),
      i_avg_basket_ratio.map(x=>x._1+"|"+x._2),
      i_basket_classes_count_sta.map(x=>x._1+"|"+x._2),
      i_basket_same_class_count_sta.map(x=>x._1+"|"+x._2))
  }

  // 时间分布 temporal distribution
  def temporal_dist(sc: SparkContext,
                    step_5_rootPath: String,
                    readPath: String,
                    index_path: Int,
                    start_index: Int, end_index: Int) = {

    def readPathSplit(pathList: String, index: Int) = step_5_rootPath + pathList.split(",")(index)
    val count_dist = sc.textFile(readPathSplit(readPath, index_path)).map{ line =>
      val tokens = line.split("\\|",-1)
      val user_id = tokens(0)
      val item_id = tokens(1)
      val dow_count = tokens.drop(2).map(_.toDouble)
      (item_id, dow_count)
    }.groupByKey().mapValues(time_count =>{
      var count_concat = ListBuffer[Double]()
      for(dow <- start_index to end_index){
        count_concat += time_count.toList.map(x =>x(dow)).sum
      }
      val all_count = count_concat.sum
      count_concat.map(StatsUtil.double_ratio(_, all_count)).mkString("|")
    })
    count_dist.map(StrUtil.tupleToString(_,"|"))
  }

  //step_5_34    i 时间特征
  // from step_5_4  ui_time_dow,ui_time_hour,ui_time_month,ui_time_season,ui_time_workday
  def get_item_time_info(data_simplified: RDD[(String, String, String, String, String, String, String)],
                         step_5_4: String,
                         step_5_rootPath: String,
                         sc: SparkContext)= {
    def readPathSplit(pathList: String, index: Int) = step_5_rootPath +
      pathList.split(",")(index)

    //i_time_workday_ratio  		字段：  item_id, ratio
    val workday_count = sc.textFile(readPathSplit(step_5_4,4)).map{line =>
      val tokens = line.split("\\|",-1)
      val item_id = tokens(1)
      val workday_count = tokens(2).toDouble
      val item_count = tokens(4).toDouble
      (item_id, (workday_count, item_count))
    }

    val i_time_workday_ratio = workday_count
      .reduceByKey((x,y) =>(x._1 + y._1, x._2 + y._2))
      .map(x =>{
        val item_id = x._1
        val count_sum = x._2
        val ratio = StatsUtil.double_ratio(count_sum._1, count_sum._2)
        item_id+"|"+ ratio
      })

    //i_order_days_ratio_ratio 卖出日期占比 字段：  item_id, ratio
    //总天数
    val total_days_sorted = data_simplified.map(_._2).sortBy(x =>x).collect()
    val total_days_count = if (total_days_sorted.size > 2) {
      DateUtil.dateTimeDiffInDay(total_days_sorted.init(0), total_days_sorted.tail(total_days_sorted.size-2))
    } else println("Times is less")

    val i_order_days_ratio_ratio = data_simplified
      .map(x => (x._4, x._2.substring(0, 10)))
      .groupByKey()
      .mapValues(date => StatsUtil.double_ratio(date.toList.distinct.size, total_days_count))

    //i_3orders_days_ratio_ratio	字段：  item_id, ratio   至少卖出3件的日期占比
    val i_3orders_days_ratio_ratio = data_simplified
      .map(x => (x._4, x._2.substring(0, 10)))
      .groupByKey()
      .mapValues(date => StatsUtil.double_ratio(
        date.toList.map(x => (x, 1))
          .groupBy(_._1)
          .mapValues(_.size).toArray
          .sortWith(_._2 > _._2).filter(x => x._2 >= 3).size, total_days_count))
    ( i_time_workday_ratio,
      i_order_days_ratio_ratio.map(x=>x._1 + "|"+x._2),
      i_3orders_days_ratio_ratio.map(x=>x._1 + "|"+x._2) )
  }

  //step_5_35   i reorder特征
  //i_reorder_info 			字段：  item_id, count, ratio, user_ratio, days_since_prior_avg
  def get_item_order_features(user_items_order_list: RDD[(String, String, Array[String])],
                              step_2_2: String,
                              reorder_top_Num: Int,
                              sc: SparkContext): (RDD[(String)], RDD[(String)]) = {

    val total_users_count = user_items_order_list.map(_._1).distinct().count()
    val i_reorder_size = user_items_order_list
      .map { case (user, item, orderOrNot) => (item, orderOrNot) }
      .groupByKey()
      .mapValues(orders_all => {
        val count = orders_all.map(list => list.filter(_ == "1").drop(1).size).sum
        val ratio = StatsUtil.double_ratio(count,
          orders_all.map(list => list.filter(_ == "1").size).sum)
        val user_ratio = StatsUtil.double_ratio(orders_all
          .map(list => list.filter(_ == "1").drop(1))
          .filter(_.size >= 1).size, total_users_count)
        (count, ratio, user_ratio)
      })

    val data_14cols_1 = sc.textFile(step_2_2).map { line =>
      val tokens = line.split("\\|", -1)
      //UID,SLDAT,user_id,PRODNO,item_id,DPTNO,DPTNAME,BNDNO,BNDNAME,QTY,AMT,
      val uid = tokens(0)
      val sldat = tokens(1)
      val user_id = tokens(2)
      val item_id = tokens(4)
      val dptno = tokens(5)
      val qty = tokens(9)
      val amt = tokens(10)
      (uid,sldat,user_id,item_id,dptno, qty, amt)
    }

    //i_reorder_days_since_prior_average 重复购买该物品的时间间隔平均值
    val days_since_prior_avg = data_14cols_1.map {
      case (uid, sldat, user_id, item_id, dptno, qty, amt) =>
        ((user_id, item_id), sldat)
    }.groupByKey().mapValues(date => {
      val tuple_date = date.toList.sortBy(x=>x).filter(_.size >1).sliding(2).toList
      val interval_list = tuple_date
        .map(x => if(x.size <= 1) 0 else DateUtil.dateTimeDiffInDay(x(0), x(1)))
      interval_list
    }).flatMapValues(x=>x)
      .map{case ((user_id,item_id), interval)=> (item_id, interval)}
      .groupByKey()
      .mapValues(interval_list=> StatsUtil.mean(interval_list.toArray.map(_.toDouble)))

    val i_reorder_info = i_reorder_size.join(days_since_prior_avg)
      .map{case (item, ((count, ratio, user_ratio), inter_avg)) =>
        (item, count, ratio, user_ratio, inter_avg)}


    //复购率最高的topN：
    //i_reorder_ratio_top_N	字段：  item_id, ratio,  days_since_prior_avg
    val i_reorder_ratio_top_N = i_reorder_info
      .sortBy(x=>x._3, false).map{ case (item, count, ratio, user_ratio, inter_avg)=>
      (item, ratio, inter_avg)}
      .take(reorder_top_Num)
    val i_reorder_ratio_top_N_rdd = sc.makeRDD(i_reorder_ratio_top_N
      .map(StrUtil.tupleToString(_,"|")))
    (i_reorder_info.map(StrUtil.tupleToString(_,"|")),
      i_reorder_ratio_top_N_rdd)
  }

  def get_i_strike(user_items_order_list: RDD[(String, String, Array[String])],
                   sc: SparkContext): RDD[(String)] = {
    //step_5_36  i strike特征  i_strik      字段：item_id, s1, s2, ........, s84
    val i_strike = user_items_order_list.map { case (user, item, orderList) =>
      (item, (user, orderList))}.groupByKey().mapValues(user_order_list => {

      //‘1to1', '11to1', '10to1', '111to1', '110to1', '101to1', '100to1'
      // count, chance, ratio  => max，min，average, median
      def cal_strike(oneToOneCount: List[(Double)]): String = {
        val oneToOneCount_max = oneToOneCount.max.toString
        val oneToOneCount_min = oneToOneCount.min.toString
        val oneToOneCount_average = StatsUtil.mean(oneToOneCount.toArray)
        val oneToOneCount_median = StatsUtil.media(oneToOneCount.toArray)
        List(oneToOneCount_max, oneToOneCount_min,
          oneToOneCount_average, oneToOneCount_median).mkString("|")
      }

      def strike_concat(pattern: String, pattern_c: String, num_buy: Int): String = {
        val user = user_order_list.map(_._1)
        val order = user_order_list.map(_._2)
        val oneToOne = order.map(_.sliding(num_buy).toList.map(_.mkString("|")))
        val oneToOneCount = oneToOne.map(_.filter(_ == pattern).size.toDouble)
        val oneToOneChance = oneToOne.map(_.filter(x => x == pattern || x == pattern_c).size.toDouble)
        val oneToOneRatio = oneToOneCount
          .zip(oneToOneChance)
          .map { case (oneToOneCount, oneToOneChance) =>
            if (oneToOneChance == 0) 0
            else StatsUtil.double_ratio(oneToOneCount, oneToOneChance)
          }

        val count = cal_strike(oneToOneCount.toList)
        val chance = cal_strike(oneToOneChance.toList)
        val ratio = cal_strike(oneToOneRatio.toList)
        List(count, chance, ratio).mkString("|")
      }
      val oo = "1|1"
      val oo_c = "1|0"

      val ooo = "1|1|1"
      val ooo_c = "1|1|0"

      val ono = "1|0|1"
      val ono_c = "1|0|0"

      val oooo = "1|1|1|1"
      val oooo_c = "1|1|1|0"

      val oono = "1|1|0|1"
      val oono_c = "1|1|0|0"

      val onoo = "1|0|1|1"
      val onoo_c = "1|0|1|0"

      val onno = "1|0|0|1"
      val onno_c = "1|0|0|0"
      List(strike_concat(oo, oo_c, 2),
        strike_concat(ooo, ooo_c, 3), strike_concat(ono, ono_c, 3),
        strike_concat(oooo, oooo_c, 4), strike_concat(oono, oono_c, 4),
        strike_concat(onoo, onoo_c, 4), strike_concat(onno, onno_c, 4)
      ).mkString("|")
    })
    i_strike.map(x=> x._1+"|"+x._2)
  }

  def get_i_trend(step_4_2_2: String,
                  trend_delta: Double,
                  sc: SparkContext): RDD[(String)] ={
    //    step_5_37   i trend 特征     输入：  step_4/item_frequency_month
    // i_trend 	字段：item_id, trend
    val item_frequency_month = sc.textFile(step_4_2_2).map { line =>
      val tokens = line.split("\\|", -1)
      //item_id, 1_month, 2_month, 3_month, 4_month, 5_month, 6_month
      val item_id = tokens(0)
      val month_1 = tokens(1).toDouble
      val month_2 = tokens(2).toDouble
      val month_3 = tokens(3).toDouble
      (item_id, getTrend(month_1, month_2, month_3, trend_delta))
    }
    item_frequency_month.map(x=>x._1+"|"+x._2)
  }

  def savePathSplit(rootPath: String, pathList: String, index: Int)= {
    val save_path = rootPath + pathList.split(",")(index)
    HadoopOpsUtil.removeDir(save_path, save_path)
    save_path
  }

  /**
   * 判断三个月的购买趋势
   * m1 > m2 > m3   or  (m1+m2)>(m2+m3) *(1+delta)    then    购买趋势=上升
      m1 < m2 < m3   or  (m1+m2)<(m2+m3) *(1-delta)    then    购买趋势=下降
   */
  def getTrend(m1: Double, m2:Double, m3: Double, delta: Double): Int ={
    val trend = if ((m1 > m2 && m2 > m3)  || (m1+m2) > (m2+m3) *(1+delta)) {
      1
    } else if ((m1 < m2 && m2 < m3) ||  (m1+m2)<(m2+m3) *(1-delta)) {
      -1
    } else 0
    trend
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("recommend") {
      opt[String]("item_total_input_path")
        .action((x, c) => c.copy(item_total_input_path = x))
      opt[String]("data_simplified_input_path")
        .action((x, c) => c.copy(data_simplified_input_path = x))
      opt[String]("user_baskets_has_item_list_input_path")
        .action((x, c) => c.copy(user_baskets_has_item_list_input_path = x))
      opt[String]("ui_time_dist_input_path")
        .action((x, c) => c.copy(ui_time_dist_input_path = x))
      opt[String]("data_14cols_1_input_path")
        .action((x, c) => c.copy(data_14cols_1_input_path = x))
      opt[String]("item_frequency_month_input_path")
        .action((x, c) => c.copy(item_frequency_month_input_path = x))
      opt[Double]("trend_delta")
        .action((x, c) => c.copy(trend_delta = x))
      opt[String]("root_output_path")
        .action((x, c) => c.copy(root_output_path = x))
      opt[String]("i_top100_output_path")
        .action((x, c) => c.copy(i_top100_output_path = x))
      opt[String]("i_price_output_path")
        .action((x, c) => c.copy(i_price_output_path = x))
      opt[String]("i_basket_output_path")
        .action((x, c) => c.copy(i_basket_output_path = x))
      opt[String]("i_time_output_path")
        .action((x, c) => c.copy(i_time_output_path = x))
      opt[String]("i_reorder_output_path")
        .action((x, c) => c.copy(i_reorder_output_path = x))
      opt[Int]("reorder_top_Num")
        .action((x, c) => c.copy(reorder_top_Num = x))
      opt[String]("i_strike_output_path")
        .action((x, c) => c.copy(i_strike_output_path = x))
      opt[String]("i_trend_output_path")
        .action((x, c) => c.copy(i_trend_output_path = x))
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
                     item_total_input_path: String ="file:///xiaoi/crm/data/step_3/item_total",
                     data_simplified_input_path: String = "file:///xiaoi/crm/data/step_3/data_simplified",
                     user_baskets_has_item_list_input_path: String = "file:///xiaoi/crm/data/step_3/user_baskets_has_item_list",
                     ui_time_dist_input_path: String = "ui_time_dow,ui_time_hour,ui_time_month,ui_time_season,ui_time_workday",
                     data_14cols_1_input_path: String = "file:///xiaoi/crm/data/step_2/data_14cols_1",
                     item_frequency_month_input_path: String = "step_4/item_frequency_month",
                     trend_delta: Double = 0.1,
                     root_output_path: String = "file:///xiaoi/crm/data/step_5/",
                     i_top100_output_path: String = "i_top100",
                     i_price_output_path: String = "i_price_sta,i_avg_price_rank_percent",
                     i_basket_output_path: String = "i_basket_size_sta,i_only_me_ratio,i_avg_basket_ratio,i_basket_classes_count_sta,i_basket_same_class_count_sta",
                     i_time_output_path: String = "i_time_dow_ratio,i_time_hour_ratio,i_time_month_ratio,i_time_season_ratio,i_time_workday_ratio,i_order_days_ratio_ratio,i_3orders_days_ratio_ratio",
                     i_reorder_output_path: String = "i_reorder_info,i_reorder_ratio_top_N",
                     reorder_top_Num: Int = 100,
                     i_strike_output_path: String = "i_strike",
                     i_trend_output_path: String = "i_trend"
                   )
}
