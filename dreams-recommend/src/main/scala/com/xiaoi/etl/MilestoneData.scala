package com.xiaoi.etl

/**
 * @Package com.xiaoi.etl
 * @author ming
 * @date 2020/4/15 17:12
 * @version V1.0
 * @description 中间数据生成
 */

import com.xiaoi.common.StrUtil.tupleToString
import com.xiaoi.common._
import com.xiaoi.conf.ConfigurationManager
import com.xiaoi.constant.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/**
 * written by josh.ye in 2018/7/2
 * step_3 中间数据生成; 部分数据可以看做是里程碑数据
 */
object MilestoneData {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("milestone data")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = sparkSession.sqlContext

    logger.info("step_3 milestoneData中间数据生成... 存储路径处理")

//    val rootPath = params.output_rootPath
//    val basic_info = savePathSplit(rootPath, params.detail_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_BASIC_INFO))
//    val item_detail = savePathSplit(rootPath, params.detail_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_ITEM_DETAIL))
//    val user_detail = savePathSplit(rootPath, params.detail_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_USER_DETAIL))
//
//    val user_uid = savePathSplit(rootPath, params.ui_pair_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_USER_UID))
//    val uid_sldat = savePathSplit(rootPath, params.ui_pair_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_UID_SLDAT))
//    val uid_item = savePathSplit(rootPath, params.ui_pair_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_UID_ITEM))
//
//    val data_simplified = params.data_simplified_output_path
//    val user_baskets_has_item_list = savePathSplit(rootPath, params.basket_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_USER_BASKET_HAS_ITEM_LIST))
//    val user_basket_money = savePathSplit(rootPath, params.basket_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_USER_BASKET_MONEY))
//    val user_total = savePathSplit(rootPath, params.basket_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_USER_TOTAL))
//
//    val item_total = savePathSplit(rootPath, params.item_class_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_ITEM_TOTAL))
//    val item_top100_by_log_count = savePathSplit(rootPath, params.item_class_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_ITEM_TOP100_BY_LOG_COUNT))
//    val item_top100_by_user_count = savePathSplit(rootPath, params.item_class_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_ITEM_TOP100_BY_USER_COUNT))
//    val class_total = savePathSplit(rootPath, params.item_class_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_CLASS_TOTAL))
//    val class_top10 = savePathSplit(rootPath, params.item_class_output_path,
//      ConfigurationManager.getInt(Constants.INDEX_CLASS_TOP10))

    // 准备 14 个字段的origin 数据
    val data_cleaned = get_data_cleaned(params.data_cleaned_input_path, sc)
    data_cleaned.cache()

    logger.info("step_3_1 基本统计  basic_info ...")
    /**
     * log_count : 交易记录数量
     * item_count: 独立商品
     * user_count: 独立用户数量
     * session_count: 交易回合数量
     * dpt_count: 独立商品种类数量
     * total_money: 总交易额
     * total_days: 总时间
     */
    val basic_count_pair: (String, Long, Long) = get_basic_info(data_cleaned, sqlContext)

    val basic_info_str = basic_count_pair._1
    val user_count = basic_count_pair._2
    val session_count = basic_count_pair._3
//    sc.makeRDD(Array(basic_info_str)).repartition(1).saveAsTextFile(basic_info)

    logger.info("step_3_2 item_detail 商品相关信息...")
    /**
     * item_id
     * prodno: 产品编码
     * pluname： 产品名称
     * dptno: 类别编码
     * dptname: 类别名称
     * bandno： 品牌编码
     * bandname: 品牌名称
     */
    val item_detail_rdd = get_item_info(data_cleaned, sc)
//    item_detail_rdd.saveAsTextFile(item_detail)

    //user_id，会员号码， 会员性别， 会员生日
    /**
     * user_id
     * vipno: 会员号码
     * vip_gender: 会员性别
     * vip_birthday: 会员生日
     * vip_created_date: 会员入会日期
     */
    logger.info("step_3_3 user_detail 用户相关信息...")
    val user_detail_rdd = get_user_info(data_cleaned, sc)
//    user_detail_rdd.saveAsTextFile(user_detail)
    data_cleaned.unpersist()

    val data_14cols_1 = read_data_14cols(sc, params.data_14cols_1_input_path)
    val data_14cols_2 = read_data_14cols(sc, params.data_14cols_2_input_path)
    val data_14cols = data_14cols_1.union(data_14cols_2).cache()
    val total_log_count = data_14cols.count()

    /**
     * userId_uid : 用户id和交易号码的对应关系
     * uid_sldat :  交易号码和交易时间（最早一次交易的时间）的对应关系
     * uid_item :   交易号码和商品id的对应关系
     */
    logger.info("step_3_10 user_uid, uid_sldat, uid_item ...")
    val uid_item_uid_triple = get_data_pair(data_14cols, sqlContext)
//    uid_item_uid_triple._1.saveAsTextFile(user_uid)
//    uid_item_uid_triple._2.saveAsTextFile(uid_sldat)
//    uid_item_uid_triple._3.saveAsTextFile(uid_item)

//    logger.info(s"step_3_31  item_total write: ${item_total}")
    /**
     * item_id
     * first_order_time : 首单日期
     * log_count
     * log_ratio ：  该item商品占总的销售数量中的占比 （该item的log_count / 总的log_count） 
     * session_count 
     * session_ratio
     * user_count
     * user_ratio
     * money
     */
    // TODO: money_ratio 也可以计算
    val item_total_rdd = get_item_total(data_14cols, total_log_count, user_count, session_count)
//    item_total_rdd.map(tupleToString(_, "|")).saveAsTextFile(item_total)
    item_total_rdd.cache()

    logger.info("step_32  item_top100_by_log_count, item_top100_by_user_count write...")
    /**
     * item_top100_by_log_count :  item_id , log_count, ratio 按照log_count 排序
     * item_top100_by_user_count:  item_id , user_count, ratio 按照user_count排序
     */
    val item_top = get_top_item(item_total_rdd, params.topn_item, sc)
//    item_top._1.saveAsTextFile(item_top100_by_log_count)
//    item_top._2.saveAsTextFile(item_top100_by_user_count)
    item_total_rdd.unpersist()

    logger.info("step_3_41  class_total  class_top10...  ")
    /**
     *  dptno
     *  log_count
     *  item_count
     *  user_count
     *  session_count
     *  money
     *  class_ratio : 该类别商品的log_count / 总的log_count
     *
     */
    val class_detail = get_class_total(data_14cols, total_log_count, params.topn_dpt, sc)
//    class_detail._1.saveAsTextFile(class_total)
//    class_detail._2.saveAsTextFile(class_top10)

//    logger.info(s"step_3_11 data_simplified write: ${data_simplified}")
    HadoopOpsUtil.removeDir(params.data_simplified_output_path, params.data_simplified_output_path)
    /**
     * uid
     * sldat
     * user_id
     * item_id
     * dptno
     * qty
     * amt
     *
     */
    // TODO: in_basket_pos: 商品在购物篮内的顺序。   in_user_pos: 用户购买物品的一个全局排序编号
    val data_simplified_rdd = get_data_simplified(data_14cols)
//    data_simplified_rdd.map(tupleToString(_, "|")).saveAsTextFile(data_simplified)
    data_14cols.unpersist()

    data_14cols_1.cache()

    //以下计算全部用hasVIPNO的数据
//    logger.info(s"step_3_21 user_baskets_has_item_list write ${user_baskets_has_item_list}")
    /**
     * user_id
     * item_id
     * basket_list
     */
    val user_basket_items = get_user_baskets_has_item_list(data_14cols_1)
//    user_basket_items.saveAsTextFile(user_baskets_has_item_list)

//    logger.info("step_3_22 user_basket_money: ", user_basket_money)
    /**
     * user_id
     * uid
     * sldat
     * basket_money : 该单的总消费额
     * basket_size :  该单包含的商品个数
     */
    val user_basket_money_rdd = get_user_basket_money(data_14cols_1)
//    user_basket_money_rdd.saveAsTextFile(user_basket_money)

//    logger.info(s"step_3_23 user_total save: ${user_total}")
    /**
     * user_id
     * first_order_time : 首单日期
     * log_count ：  商品记录数
     * item_count ： 独立商品总数
     * basket_count ： 交易回合总数
     * money ： 交易额总数
     */
    val user_total_rdd = get_user_total(data_14cols_1)
//    user_total_rdd.saveAsTextFile(user_total)

    sc.stop()
  }

  // Reading data_cleaned  14 fields
  def get_data_cleaned(step_1_1: String, sc: SparkContext):
  RDD[(String, String, String, String, String, String, String,
    String, String, String, String, String, String, String)] = {

    //step_1_1 data_cleaned
    val data_cleaned = sc.textFile(step_1_1).map { line =>
      val tokens = line.split("\\|", -1)

      //UID,SLDAT,VIPNO,PRODNO,PLUNAME,DPTNO,DPTNAME,BNDNO,BNDNAME,
      // QTY,AMT,VIP_GENDER,VIP_BIRTHDAY,VIP_CREATED_DAT
      val UID = tokens(ConfigurationManager.getInt(Constants.FIELD_UID))
      val SLDAT = tokens(ConfigurationManager.getInt(Constants.FIELD_SLDAT))
      val VIPNO = tokens(ConfigurationManager.getInt(Constants.FIELD_VIPID))
      val PRODNO = tokens(ConfigurationManager.getInt(Constants.FIELD_PRODNO))
      val PLUNAME = tokens(ConfigurationManager.getInt(Constants.FIELD_PLUNAME))
      val DPTNO = tokens(ConfigurationManager.getInt(Constants.FIELD_DPTNO))
      val DPTNAME = tokens(ConfigurationManager.getInt(Constants.FIELD_DPTNAME))
      val BNDNO = tokens(ConfigurationManager.getInt(Constants.FIELD_BNDNO))
      val BNDNAME = tokens(ConfigurationManager.getInt(Constants.FIELD_BNDNAME))
      val QTY = tokens(ConfigurationManager.getInt(Constants.FIELD_QTY))
      val AMT = tokens(ConfigurationManager.getInt(Constants.FIELD_AMT))
      val VIP_GENDER = tokens(ConfigurationManager.getInt(Constants.FIELD_VIP_GENDER))
      val VIP_BIRTHDAY = tokens(ConfigurationManager.getInt(Constants.FIELD_VIP_BIRTHDAY))
      val VIP_CREATED_DATE = tokens(ConfigurationManager.getInt(Constants.FIELD_VIP_CREATED_DATE))
      (UID, SLDAT, VIPNO, PRODNO, PLUNAME, DPTNO, DPTNAME, BNDNO, BNDNAME,
        QTY, AMT, VIP_GENDER, VIP_BIRTHDAY, VIP_CREATED_DATE)
    }
    data_cleaned
  }

  // calculate basic_info
  def get_basic_info(data_cleaned: RDD[(String, String, String, String, String, String,
    String, String, String, String, String, String, String, String)], sqlContext: SQLContext) = {

    //step_3_1 基本统计 basic_info=>log_count,item_count,user_count,session_count,dpt_count,
    // total_money,total_days
    // 交易记录数量
    val data_cleaned_log_count = data_cleaned.count()

    import sqlContext.implicits._
    val data_cleanedDF = data_cleaned.toDF("uid", "sldat", "vipno", "prodno",
      "prodname", "dptno", "dptname", "bndno", "bndname", "qty", "amt", "vip_gender",
      "vip_birthday", "vip_created_day").toDF.cache()
    val item_count = data_cleanedDF.select("prodname")
      .distinct().count()
    val user_count = data_cleanedDF.filter($"vipno" !== "")
      .select("vipno").distinct().count()
    val session_count = data_cleanedDF.select("uid")
      .distinct().count()
    val dpt_count = data_cleanedDF.select("dptno")
      .distinct().count()
    val total_money = data_cleanedDF.select("amt").rdd
      .filter(x => !x(0).equals("")).map(x => x(0).toString.toDouble)
      .sum().formatted("%.2f")
    val time_list = data_cleanedDF.select("sldat").rdd
      .map(x => x(0).toString)

    val total_days = DateUtil.dateTimeDiffInDay(time_list.min(), time_list.max())
    val basic_info_str = List(data_cleaned_log_count, item_count, user_count,
      session_count, dpt_count, total_money, total_days).mkString("|")

    data_cleanedDF.unpersist()
    (basic_info_str, user_count, session_count)
  }

  // Calculate item_info
  def get_item_info(data_cleaned: RDD[(String, String, String, String, String, String,
    String, String, String, String, String, String, String, String)], sc: SparkContext): RDD[(String)] = {

    //item_id, 产品编码, 产品名称,类别编码,品牌编码,品牌名称
    val item_detail_rdd = data_cleaned.map {
      case (uid, sldat, vipno, prodno, pluname, dptno, dptname, bndno, bndname,
      qty, amt, vip_gender, vip_birthday, vip_created_day) =>
        (prodno, prodno, pluname, dptno, dptname, bndno, bndname)
    }
      .distinct()
    item_detail_rdd.map(tupleToString(_, "|"))
  }

  // Calculate user_info
  def get_user_info(data_cleaned: RDD[(String, String, String, String, String, String, String,
    String, String, String, String, String, String, String)], sc: SparkContext): RDD[(String)] = {

    //user_id，会员号码， 会员性别， 会员生日
    val user_detail_rdd = data_cleaned
      .filter(x => !x._3.equals("")).map {
      case (uid, sldat, vipno, prodno, pluname, dptno, dptname, bndno, bndname,
      qty, amt, vip_gender, vip_birthday, vip_created_day) =>
        (vipno, vipno, vip_gender, vip_birthday, vip_created_day)
    }
      .distinct()
    user_detail_rdd.map(tupleToString(_, "|"))
  }

  // Get data_14cols
  def read_data_14cols(sc: SparkContext, data_14cols_path: String):
  RDD[(String, String, String, String, String, String, String, String)] = {

    //Input step_2_2_data_14cols_1 +2 采用全部数据包括VIPNO为空的，特定情况过滤
    sc.textFile(data_14cols_path).map { line =>
      val tokens = line.split("\\|", -1)
      //UID,SLDAT,VIPNO,PRODNO,PLUNAME,DPTNO,DPTNAME,BNDNO,BNDNAME,
      // QTY,AMT,VIP_GENDER,VIP_BIRTHDAY,VIP_CREATED_DAT
      val UID = tokens(ConfigurationManager.getInt(Constants.FIELD_UID))
      val SLDAT = tokens(ConfigurationManager.getInt(Constants.FIELD_SLDAT))
      val user_id = tokens(ConfigurationManager.getInt(Constants.FIELD_VIPID))
      val item_id = tokens(ConfigurationManager.getInt(Constants.FIELD_PLUNAME)) //FIELD_PRODNO替换为PLUNAME
      val DPTNO = tokens(ConfigurationManager.getInt(Constants.FIELD_DPTNO))
      val DPTNAME = tokens(ConfigurationManager.getInt(Constants.FIELD_DPTNAME))
      val QTY = tokens(ConfigurationManager.getInt(Constants.FIELD_QTY))
      val AMT = tokens(ConfigurationManager.getInt(Constants.FIELD_AMT))
      (UID, SLDAT, user_id, item_id, DPTNO, DPTNAME, QTY, AMT)
    }
  }

  // user_uid   uid_sldat   uid_item
  def get_data_pair(data_14cols: RDD[(String, String, String, String, String, String, String, String)],
                    sqlContext: SQLContext): (RDD[(String)], RDD[(String)], RDD[(String)]) = {

    //step_3_10   交易中间数据
    //user_uid    字段：  user_Id, uid ....     用户id和交易号码的对应关系    (交易号码按大小排序)
    //uid_sldat   字段：  uid, sldat           交易号码和交易时间的对应关系
    //uid_item    字段： uid, item_id, ....     交易号码和商品id 的对应关系
    import sqlContext.implicits._
    val data_fields = data_14cols
      .toDF("uid", "sldat", "user_id", "item_id", "dptno", "dptname", "qty", "amt")
    val user_uid_rdd = data_fields
      .filter($"user_id" !== "")
      .select("user_id", "uid").rdd
      .map(x => (x(0).toString, x(1).toString.toLong))
      .repartition(1).sortBy(_._2)
      .map(tupleToString(_, "|"))
      .distinct()
    val uid_sldat_rdd = data_fields
      .select("uid", "sldat").rdd
      .map(x => (x(0).toString, x(1).toString))
      .groupByKey().mapValues(_.toList.sortWith(_ < _)(0)) //交易回合中取第一时间
      .map(tupleToString(_, "|"))
      .distinct()
    val uid_item_rdd = data_fields
      .select("uid", "item_id").rdd
      .map(x => (x(0).toString, x(1).toString))
      .map(tupleToString(_, "|"))
      .distinct()
    (user_uid_rdd, uid_sldat_rdd, uid_item_rdd)
  }

  def get_item_total(data_14cols:
                     RDD[(String, String, String, String, String, String, String, String)],
                     total_log_count: Long,
                     user_sum: Long,
                     session_sum: Long) = {

    //step_3_31      商品交易记录次数和金额汇总   item_total
    //字段: item_id, first_order_time, log_count, log_ratio, ++
    //session_count, session_ratio, user_count, user_ratio,  money
    //log_ratio: 该item商品占总的销售数量总的占比 （该item的log_count/总的log_count)
    //money_ratio也可以算， 根据计算量待定
    val item_total_rdd = data_14cols
      .map { case (uid, sldat, user_id, item_id, dptno, dptname, qty, amt) =>
        (item_id, (user_id, uid, amt, sldat))
      }
      .groupByKey()
      .mapValues(x => {
        val first_order_time = x.map(_._4).toList.sortBy(x => x).take(1)(0)
        val log_count = x.map(_._1).size
        val log_ratio = StatsUtil.double_ratio(log_count, total_log_count)
        val user_count = x.map(_._1).filter(x => !x.equals("")).toList.distinct.size //hasVIPNO
        val user_ratio = StatsUtil.double_ratio(user_count, user_sum)
        val session_count = x.map(_._2).toList.distinct.size
        val session_ratio = StatsUtil.double_ratio(session_count, session_sum)
        val money = x.filter(!_._3.equals("")).map(_._3.toDouble).sum.formatted("%.2f")
        (first_order_time, log_count, log_ratio, user_count, session_count, money, user_ratio, session_ratio)
      }).map { case (item_id, (first_order_time, log_count, log_ratio, user_count,
    session_count, money, user_ratio, session_ratio)) =>
      (item_id, first_order_time, log_count, log_ratio,
        session_count, session_ratio, user_count, user_ratio, money)
    }
    item_total_rdd
  }

  def get_top_item(item_total_rdd: RDD[(String, String, Int, Double, Int, Double, Int,
    Double, String)], itemTopNum: Int, sc: SparkContext) = {

    //step_3_32    最受欢迎的N大商品   (N是参数， 对应文件名来说是个确定的值)
    //item_top100_by_log_count: 		字段：item_id, log_count, ratio          按照log_count排序
    //  item_top100_by_user_count: 		字段：item_id, user_count, ratio        按照user_count排序
    //  输入:   step_3/item_total
    //
    val item_top100_by_log_count_list = item_total_rdd
      .map { case (item_id, first_order_time, log_count, log_ratio,
      session_count, session_ratio, user_count, user_ratio, money) =>
        (item_id, log_count, log_ratio)
      }
      .sortBy(x => x._2, false)
      .take(itemTopNum)

    val item_top100_by_user_count_list = item_total_rdd
      .map { case (item_id, first_order_time, log_count, log_ratio,
      session_count, session_ratio, user_count, user_ratio, money) =>
        (item_id, user_count, user_ratio)
      }
      .sortBy(x => x._2, false)
      .take(itemTopNum)

    (sc.parallelize(item_top100_by_log_count_list, 1)
      .map(tupleToString(_, "|")),
      sc.parallelize(item_top100_by_user_count_list, 1)
        .map(tupleToString(_, "|")))
  }

  def get_class_total(data_14cols: RDD[(String, String, String, String, String, String,
    String, String)], total_log_count: Long, dptTopNum: Int, sc: SparkContext) = {

    //    step_3_41       类别交易次数和金额汇总   class_total
    //    字段：	dptno, log_count, item_count,  user_count, session_count, money,  class_ratio
    //    user_count:   独立用户数
    //    session_count:   独立交易回合数
    //    class_ratio:   该类别商品的log_count/总的log_count
    val class_total_rdd = data_14cols
      .map { case (uid, sldat, user_id, item_id, dptno, dptname, qty, amt) =>
        (dptno, (user_id, item_id, uid, amt))
      }
      .groupByKey()
      .mapValues(x => {
        val log_count = x.map(_._1).size
        val item_count = x.map(_._2).toList.distinct.size
        val user_count = x.map(_._1).toList
          .filter(x => !x.equals("")).distinct.size //hasVIPNO
        val session_count = x.map(_._3).toList.distinct.size
        val money = x.filter(!_._4.equals("")).map(_._4.toDouble).sum.formatted("%.2f")
        val class_ratio = StatsUtil.double_ratio(log_count, total_log_count)
        (log_count, item_count, user_count, session_count, money, class_ratio)
      }).map {
      case (dptno, (log_count, item_count, user_count, session_count, money, class_ratio)) =>
        (dptno, log_count, item_count, user_count, session_count, money, class_ratio)
    }.cache()

    //step_3_42 最畅销的商品类别 :    classes_top10
    //字段： dptno， dptname，  class_ratio   按class_ratio排序， 取前10 (10实际是参数)
    val classes_top10_rdd = sc
      .makeRDD(class_total_rdd.sortBy(x => x._7, false).take(dptTopNum))
      .map { case (dptno, log_count, item_count, user_count, session_count, money, class_ratio) =>
        (dptno, class_ratio)
      }
      .join(data_14cols.map { case (uid, sldat, user_id, item_id, dptno, dptname, qty, amt) =>
        (dptno, dptname)
      })
      .map { case (dptno, (class_ratio, dptname)) => (dptno, dptname, class_ratio) }
      .distinct()
      .sortBy(_._3, false).repartition(1)
    (class_total_rdd.map(tupleToString(_, "|")),
      classes_top10_rdd.map(tupleToString(_, "|")).repartition(1))
  }

  //step_3_11 交易数据的简化版
  // data_simplified （里程碑）uid, sldat,user_id， item_id， dptno, qty, amt, in_basket_pos, in_user_pos
  def get_data_simplified(data_14cols: RDD[(String, String, String, String, String, String, String, String)]) = {
    val data_simplified_rdd = data_14cols
      .map { case (uid, sldat, user_id, item_id, dptno, dptname, qty, amt) =>
        (uid, sldat, user_id, item_id, dptno, qty, amt)
      }
    data_simplified_rdd
  }

  // TODO: 可以优化
  def get_user_baskets_has_item_list(data_14cols_1: RDD[(String, String, String, String,
    String, String, String, String)]): RDD[(String)] = {

    // 购物篮含item数据        user_baskets_has_item_list
    val user_baskets_has_item_list_rdd = data_14cols_1
      .map { case (uid, sldat, user_id, item_id, dptno, dptname, qty, amt) =>
        (user_id, (item_id, uid, sldat))
      }
      .groupByKey()
      .mapValues(x => {
        val item_uid_time = x
        val uid_items_group = item_uid_time.toList.groupBy(x => x._1)
        val items = uid_items_group.keys.toList
        val uids = item_uid_time.toList.sortBy(x => x._3).map(_._2).distinct
        var order_list = ListBuffer[Int]()
        val order = ListBuffer[(String, String)]()
        for (item <- items) {
          order_list.clear()
          for (uid <- uids) {
            if (uid_items_group.get(item).getOrElse(List(("", "", ""))).map(_._2).contains(uid))
              order_list += 1
            else order_list += 0
          }
          order += Tuple2(item, order_list.mkString("|"))
        }
        order
      }).flatMapValues(x => x)
      .map(x => x._1 + "|" + x._2._1 + "|" + x._2._2)
    user_baskets_has_item_list_rdd
  }

  /**
   * step_3_22      用户购物篮消费额数据    user_basket_money   （里程碑，经典的rfm模型可以从这里取数据）
   * user_id, uid, sldat, basket_money, basket_size
   * basket_money: 该单的总消费额，不涉及具体的item)
   * basket_size:  该单包含的商品个数
   */
  def get_user_basket_money(data_14cols_1:
                            RDD[(String, String, String, String, String, String, String, String)]): RDD[(String)] = {

    val user_group = data_14cols_1
      .map { case (uid, sldat, user_id, item_id, dptno, dptname, qty, amt) =>
        (user_id, item_id, uid, sldat, amt)
      }

    val uid_amtSum_count = user_group
      .map { case (user_id, item_id, uid, sldat, amt) =>
        ((user_id, uid), (amt, item_id))
      }
      .groupByKey()
      .mapValues(x => {
        val basket_money = x.filter(!_._1.equals(""))
          .map(_._1.toDouble).sum.formatted("%.2f")
        val basket_size = x.map(_._2).size
        (basket_money, basket_size)
      })

    val user_basket_money_rdd = user_group
      .map { case (user_id, item_id, uid, sldat, amt) =>
        ((user_id, uid), sldat)
      }.groupByKey().mapValues(x => x.toList.sortWith(_ < _)(0)) //每个交易回合时间取1个
      .join(uid_amtSum_count)
      .map { case ((user_id, uid), (sldat, (basket_money, basket_size))) =>
        (user_id, uid, sldat, basket_money, basket_size)
      }
      .distinct()
    user_basket_money_rdd
      .map(tupleToString(_, "|"))
  }

  /**
   * step_3_23      用户交易次数和金额汇总     user_total
   * user_id, first_order_time, log_count, item_count, basket_count, money
   * 首单日期, 商品记录数， 独立商品总数， 交易回合总数， 交易额总数
   */
  def get_user_total(data_14cols_1:
                     RDD[(String, String, String, String, String, String, String, String)]): RDD[(String)] = {

    val user_total_rdd = data_14cols_1
      .map { case (uid, sldat, user_id, item_id, dptno, dptname, qty, amt) =>
        (user_id, (item_id, uid, amt, sldat))
      }
      .groupByKey()
      .mapValues(x => {
        val first_order_time = x.map(_._4).toList.sortBy(x => x).take(1)(0)
        val log_count = x.map(_._1).size
        val item_count = x.map(_._1).toList.distinct.size
        val session_count = x.map(_._2).toList.distinct.size
        val money = x.filter(!_._3.equals("")).map(_._3.toDouble).sum.formatted("%.2f")
        (first_order_time, log_count, item_count, session_count, money)
      }).map { case (user_id, (first_order_time, log_count, item_count, basket_count, money)) =>
      (user_id, first_order_time, log_count, item_count, basket_count, money)
    }
    user_total_rdd
      .map(tupleToString(_, "|"))
  }

  // 一个参数输入多个表名并拆分去存
  def savePathSplit(rootPath: String, pathList: String, index: Int) = {
    var save_path = ""
    try {
      save_path = rootPath + pathList.split(",")(index)
    } catch {
      case e: Exception => println(s"Paths split error: $pathList")
    }
    HadoopOpsUtil.removeDir(save_path, save_path)
    save_path
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_3") {
      opt[String]("data_cleaned_input_path")
        .action((x, c) => c.copy(data_cleaned_input_path = x))
      opt[String]("pluname2id_input_path")
        .action((x, c) => c.copy(pluname2id_input_path = x))
      opt[String]("vipno2id_input_path")
        .action((x, c) => c.copy(vipno2id_input_path = x))
      opt[String]("data_14cols_1_input_path")
        .action((x, c) => c.copy(data_14cols_1_input_path = x))
      opt[String]("data_14cols_2_input_path")
        .action((x, c) => c.copy(data_14cols_2_input_path = x))
      opt[String]("output_rootPath")
        .action((x, c) => c.copy(output_rootPath = x))
      opt[String]("detail_output_path")
        .action((x, c) => c.copy(detail_output_path = x))
      opt[String]("ui_pair_output_path")
        .action((x, c) => c.copy(ui_pair_output_path = x))
      opt[String]("data_simplified_output_path")
        .action((x, c) => c.copy(data_simplified_output_path = x))
      opt[String]("basket_output_path")
        .action((x, c) => c.copy(basket_output_path = x))
      opt[String]("item_class_output_path")
        .action((x, c) => c.copy(item_class_output_path = x))
      opt[Int]("topn_item")
        .action((x, c) => c.copy(topn_item = x))
      opt[Int]("topn_dpt")
        .action((x, c) => c.copy(topn_dpt = x))
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
                     data_cleaned_input_path: String = "data/step_1_1/data_cleaned",
                     pluname2id_input_path: String = "data/step_2_1/Pluname2id",
                     vipno2id_input_path: String = "data/step_2_1/Vipno2id",
                     data_14cols_1_input_path: String = "",
                     data_14cols_2_input_path: String = "",
                     output_rootPath: String = "",
                     detail_output_path: String = "",
                     ui_pair_output_path: String = "",
                     data_simplified_output_path: String = "",
                     basket_output_path: String = "",
                     item_class_output_path: String = "",
                     topn_item: Int = 100,
                     topn_dpt: Int = 10
                   )

}
