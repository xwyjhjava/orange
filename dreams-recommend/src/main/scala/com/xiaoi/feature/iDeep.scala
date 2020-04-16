package com.xiaoi.feature

import com.xiaoi.common.{DateUtil, HadoopOpsUtil, StatsUtil}
import com.xiaoi.common.StrUtil.tupleToString
import com.xiaoi.conf.ConfigurationManager
import com.xiaoi.constant.Constants
import com.xiaoi.etl.MilestoneData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.mutable.ListBuffer

/**
  * created by josh in 2018/8/8
  * 挖掘一些较深入的特征   有些特征做的不完备
  */
object iDeep {
  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val INDEX_AVG_IN_ITEM_FREQUENCY = 34

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName("step6_1to7 or iDeep")
    val sc = new SparkContext(conf)
    removePath(params)

    val itemFrequency = get_item_frequency(params.item_frequency_day_input_path, sc)
    itemFrequency.cache()
    val pluname2id = sc.textFile(params.pluname2id_input_path)
      .map(x => {
        val list = x.split("\\|", -1)
        (list(0), list(1)) //id,pluname
      }).collectAsMap()

    logger.info("i_removed_suddenly")
    val removed_suddenly = get_i_removed_suddenly(itemFrequency,
      params.removed_suddenly_Tn_input)
    removed_suddenly.saveAsTextFile(params.i_removed_suddenly_output_path)

    logger.info("i_sale_drop")
    val itemSaleDrop = get_i_sale_drop(itemFrequency,
      params.sale_drop_Tn_input)
    itemSaleDrop.saveAsTextFile(params.i_sale_drop_output_path)

    logger.info("i_sale_up")
    val itemSaleUp = get_i_sale_up(itemFrequency, params.sale_up_Tn_input)
    itemSaleUp.saveAsTextFile(params.i_sale_up_output_path)

    logger.info("step_6_6  i_time_dow_top_N ")
    val data_14cols_1 = MilestoneData.read_data_14cols(sc, params.data_14cols_1)
    val data_14cols_2 = MilestoneData.read_data_14cols(sc, params.data_14cols_2)
    val data_14cols = data_14cols_1.union(data_14cols_2)
    val dowTopN = get_i_time_topN(data_14cols, params.topn_entropy)
    dowTopN.saveAsTextFile(params.i_time_dow_top_N_output_path)

    logger.info("step_6_7  周期性商品挖掘")
    val filterData = getCyclicalItems(data_14cols_1)
    filterData.cache()
    filterData.filter(x => x._2 >= 1 && x._2 <= 30)
    filterData.filter(x => x._2 > 30 && x._2 <= 90)
    filterData.filter(x => x._2 > 90 && x._2 <= 365)
    filterData.unpersist()

    logger.info("根据交易item进行用户分群")
    val dataCleanedHasVIPNO = "file:///xiaoi/crm/data/step_2/data_14cols_1"
    val savePath = "file:///xiaoi/josh/data/cluster"

    logger.info("新鲜商品模型的准确度测试")
    //UID, SLDAT, user_id, item_id, DPTNO,DPTNAME, QTY, AMT
    val test_month = "2018-03"
    val allRDD = data_14cols_1.map(x => (x._3, x._4, x._2)) //user,item,time
    val testRDD = allRDD.map {
      case (user, item, time) => (user, (item, time))
    }.groupByKey().mapValues(x => {
      val allTestData = x.filter(_._2.substring(0, 7) == test_month).map(_._1)
      val trainData = x.filter(_._2.substring(0, 7) != test_month).map(_._1)
      allTestData.toSet -- trainData.toSet
    }).filter(_._2.size > 0).flatMapValues(x => x)
    testRDD.map(x => (pluname2id.get(x._2).get, x._2)).take(10)

    sc.stop()
  }

  // 时间轴最显著商品
  def get_i_time_topN(data_14cols:
                      RDD[(String, String, String, String, String, String, String, String)],
                      topN: Int) = {
    //UID, SLDAT, user_id, item_id, DPTNO,DPTNAME, QTY, AMT)
    val skuAndTime = data_14cols.map(x => { //pluname, sldat
      val pluname = x._4
      val sldat = x._2
      val weekDay = DateUtil.getWeekDayFromStr(sldat)
      (pluname, weekDay)
    }).cache()
    var topEntropyList = ListBuffer[(Array[(String)])]()
    for (theDay <- 1 to 7) {
      val oneDay = skuAndTime.filter(_._2 == theDay).map(_._1).cache()
      //skus in oneDay
      val count = oneDay.count()
      val rdd = oneDay.map((_, 1))
        .reduceByKey(_ + _)
        .map(x => (x._1, StatsUtil.double_ratio(x._2, count)))
      val list = rdd.collect().toList
      val plunoEntropy = rdd.map(x => x._1).map(item =>
        (theDay, item, calcInfo_Gini_fromRatio(item, list)))
        .sortBy(x => x._3, false).take(topN)
      topEntropyList += plunoEntropy.map(tupleToString(_, "|"))
      oneDay.unpersist()
    }
    data_14cols.sparkContext.makeRDD(topEntropyList.reduce(_ ++ _))
  }

  def removePath(params: Params) = {
    for (path <- List(params.i_removed_suddenly_output_path,
      params.i_sale_up_output_path,
      params.i_sale_drop_output_path,
      params.i_necessities_special_output_path,
      params.i_necessities_output_path,
      params.i_expensive_necessities_output_path,
      params.i_expensive_luxury_output_path,
      params.i_coldest_output_path,
      params.i_time_dow_top_N_output_path,
      params.i_time_hour_top_N_output_path,
      params.i_time_month_top_N_output_path,
      params.i_time_season_top_N_output_path,
      params.i_time_workday_top_N_output_path)) {
      HadoopOpsUtil.removeDir(path, path)
    }
  }

  //  突然下架商品挖掘
  def get_i_removed_suddenly(item_frequency: RDD[Array[String]],
                             Tn_input: String) = {
    val TnList = Tn_input.split(",")
    val (t1, t2, t3) = (TnList(0).toDouble, TnList(1).toDouble, TnList(2).toInt)
    val filterRDD = item_frequency
      .filter(x => x(INDEX_AVG_IN_ITEM_FREQUENCY).toDouble > t1)
      .filter(x => {
        val nums = x.drop(1).dropRight(5)
        (nums.filter(_ != 0).size / nums.size) > t2
      })
      .filter(x => x(t3).toDouble == 0.0 && x(t3 - 1).toDouble == 0.0)
      .map(_ (1))
    filterRDD
  }


  def get_item_frequency(item_frequency_day_input_path: String,
                         sc: SparkContext) = {
    val item_frequency_list = sc.textFile(item_frequency_day_input_path)
      .map(x => x.split("\\|", -1))
    item_frequency_list
  }

  //  销量明显下降商品挖掘
  def get_i_sale_drop(item_frequency: RDD[Array[String]],
                      Tn_input: String) = {

    val TnList = Tn_input.split(",")
    val (t1, t2, t3) = (TnList(0).toDouble, TnList(1).toDouble, TnList(2).toInt)
    val filterRDD = item_frequency
      .filter(x => x(INDEX_AVG_IN_ITEM_FREQUENCY).toDouble > t1)
      .filter(x => {
        val nums = x.drop(1).dropRight(5).map(_.toDouble)
        (nums.filter(_ != 0).size / nums.size) > t2
      })
      .filter(x => x(t3).toDouble == 0.0 && x(t3 - 1).toDouble == 0.0)
      .map(_ (1))
    filterRDD
  }

  //  销量明显上升商品挖掘
  def get_i_sale_up(item_frequency: RDD[Array[String]], Tn_input: String) = {

    val TnList = Tn_input.split(",")
    val (t1, t2, t3) = (TnList(0).toDouble, TnList(1).toDouble, TnList(2).toInt)
    val filterRDD = item_frequency
      .filter(x => x(INDEX_AVG_IN_ITEM_FREQUENCY).toDouble > t1)
      .filter(x => {
        val nums = x.drop(1).dropRight(5).map(_.toDouble)
        (nums.filter(_ != 0).size / nums.size) > t2
      })
      .filter(x => {
        val diff = x.drop(1).take(t3).map(_.toDouble)
          .sliding(2).map(t => t(0) - t(1))
        if (diff.filter(_ > 0).size == t3 / 2) true
        else false
      })
      .map(_ (0).toString)
      .distinct()
    filterRDD
  }

  // 周期性商品挖掘
    //UID, SLDAT, user_id, item_id, DPTNO,DPTNAME, QTY, AMT)
  def getCyclicalItems(data_14cols_1: RDD[(String, String, String, String, String, String, String, String)]) = {

    val skuAndTime = data_14cols_1.map(x => { //pluname, sldat
      val vipno = x._3
      val pluname = x._4
      val sldat = x._2
      ((vipno, pluname), sldat)
    }).distinct().cache()

    val filterData = skuAndTime.map(_._1)
      .groupByKey().filter(x => x._2.size > 100).flatMapValues(x => x)
      .map(x => (x, 1)).reduceByKey(_ + _)
      .filter(_._2 >= 3)

    val moreData = skuAndTime.join(filterData)
      .map { case ((user, item), (time, count)) => ((user, item), time) }
      .groupByKey()
      .mapValues(iter =>
        StatsUtil.mean(iter.toList.sortWith(_ < _).sliding(2)
          .map(x => DateUtil.dateTimeDiffInDay(x(0), x(1)).toDouble).toArray))
    skuAndTime.unpersist()
    moreData
  }

  // 用户分群所需要的字段提取 input: data_14cols_1
  val date_filter_enabled = true
  val start_date = "2018-03-01"
  val end_date = "2018-03-31"

  def getUserInfoForCluster(data_hasVIPNO: String, sc: SparkContext) = {
    sc.textFile(data_hasVIPNO)
      .filter(x => {
        if (date_filter_enabled) {
          //过滤 state_data-end_date
          val field_sldat = x.split("\\|")(ConfigurationManager.getInt(Constants.FIELD_SLDAT))
          field_sldat >= start_date + " 00:00:00" &&
            field_sldat <= DateUtil.getTimeBefore(end_date + " 00:00:00", -1)
        } else true
      })
      .map { line =>
        val tokens = line.split("\\|", -1)
        //UID,SLDAT,VIPNO,PRODNO,PLUNAME,DPTNO,DPTNAME,BNDNO,BNDNAME,
        // QTY,AMT,VIP_GENDER,VIP_BIRTHDAY,VIP_CREATED_DAT
        val user_id = tokens(ConfigurationManager.getInt(Constants.FIELD_VIPID))
        val item_id = tokens(ConfigurationManager.getInt(Constants.FIELD_PRODNO))
        val sex = tokens(ConfigurationManager.getInt(Constants.FIELD_VIP_GENDER))
        val age = tokens(ConfigurationManager.getInt(Constants.FIELD_VIP_BIRTHDAY))
        val ageIndex = getAge(DateUtil.getAgeFromBirth(age))
        (user_id, item_id, sex, ageIndex)
      }
  }

  // 根据年龄层次获得数值
  def getAge(str_age: String): Int = {
    val age = str_age.trim()
    if (age == "None" || age == "") 2
    else if (age.toFloat < 20)
      0
    else if (age.toFloat < 30)
      1
    else if (age.toFloat < 40)
      2
    else if (age.toFloat < 50)
      3
    else if (age.toFloat < 60)
      4
    else 5
  }

  def calcEntropy_fromRatio(item_ratio: List[(String, Double)]) = {
    var shannoEnt = 0.0
    for (value <- item_ratio) {
      val prob = value._2
      shannoEnt -= prob * math.log(prob) / math.log(2)
    }
    shannoEnt
  }

  def calcEntropy_fromRatio(item_ratio: RDD[(String, Double)]) = {
    var shannoEnt = item_ratio.sparkContext.accumulator(0.0)
    item_ratio.foreachPartition(partition => {
      partition.foreach { x => {
        val prob = x._2
        shannoEnt.add(-prob * math.log(prob) / math.log(2))
      }
      }
    })
    shannoEnt.value
  }

  // 已知该商品的占比 计算信息增益
  def calcInfo_Gini_fromRatio(prod: String, lst: List[(String, Double)]) = {
    val base_ent = calcEntropy_fromRatio(lst)
    val condition_ent = calcEntropy_fromRatio(lst.filter(x => x._1 != prod))
    base_ent - condition_ent
  }

  // 已知该商品的占比 计算信息增益
  def calcInfo_Gini_fromRatio(prod: String, lst: RDD[(String, Double)]) = {
    val base_ent = calcEntropy_fromRatio(lst)
    val condition_ent = calcEntropy_fromRatio(lst.filter(x => x._1 != prod))
    base_ent - condition_ent
  }

  // 计算一个商品List的信息熵 ∑(P(x)logP(x))
  def calcEntropy(list: List[(String)]) = {
    val numEntries = list.size
    val prod_count = list.map((_, 1)).groupBy(_._1).mapValues(_.size).toArray
    var shannoEnt = 0.0
    for (value <- prod_count) {
      val prob = value._2.toDouble / numEntries
      shannoEnt -= prob * math.log(prob) / math.log(2)
    }
    shannoEnt
  }

  // 计算一个商品List的信息熵 Input: RDD[List]
  def calcEntropy(rdd: RDD[(String)]) = {
    val numEntries = rdd.count
    val prod_count = rdd.map((_, 1)).reduceByKey(_ + _)
    var shannoEnt = rdd.sparkContext.accumulator(0.0)
    prod_count.foreachPartition(partition => {
      partition.foreach { x => {
        val prob = x._2.toDouble / numEntries
        shannoEnt.add(-prob * math.log(prob) / math.log(2))
      }
      }
    })
    shannoEnt.value
  }

  /**
    * 某个元素的信息增益 ex:
    * calcInfo_Gini("4", List("2","3","4","9","4"))
    */
  def calcInfo_Gini(prod: String, lst: List[(String)]) = {
    val base_ent = calcEntropy(lst)
    val condition_ent = calcEntropy(lst.filter(_ != prod))
    base_ent - condition_ent
  }

  /**
    * 某个元素的信息增益 ex:
    * calcInfo_Gini("4", RDD(String))
    */
  def calcInfo_Gini(prod: String, lst: RDD[(String)]) = {
    val base_ent = calcEntropy(lst)
    val condition_ent = calcEntropy(lst.filter(_ != prod))
    base_ent - condition_ent
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("step_3") {
      opt[String]("data_14cols_1")
        .action((x, c) => c.copy(data_14cols_1 = x))
      opt[String]("data_14cols_2")
        .action((x, c) => c.copy(data_14cols_2 = x))
      opt[String]("pluname2id_input_path")
        .action((x, c) => c.copy(pluname2id_input_path = x))
      opt[String]("item_frequency_day_input_path")
        .action((x, c) => c.copy(item_frequency_day_input_path = x))
      opt[String]("removed_suddenly_Tn_input")
        .action((x, c) => c.copy(removed_suddenly_Tn_input = x))
      opt[String]("i_removed_suddenly_output_path")
        .action((x, c) => c.copy(i_removed_suddenly_output_path = x))
      opt[String]("i_sale_up_output_path")
        .action((x, c) => c.copy(i_sale_up_output_path = x))
      opt[String]("i_sale_drop_output_path")
        .action((x, c) => c.copy(i_sale_drop_output_path = x))
      opt[String]("i_necessities_output_path")
        .action((x, c) => c.copy(i_necessities_output_path = x))
      opt[String]("i_necessities_special_output_path")
        .action((x, c) => c.copy(i_necessities_special_output_path = x))
      opt[String]("i_expensive_necessities_output_path")
        .action((x, c) => c.copy(i_expensive_necessities_output_path = x))
      opt[String]("i_expensive_luxury_output_path")
        .action((x, c) => c.copy(i_expensive_luxury_output_path = x))
      opt[String]("i_coldest_output_path")
        .action((x, c) => c.copy(i_coldest_output_path = x))
      opt[Int]("topn_entropy")
        .action((x, c) => c.copy(topn_entropy = x))
      opt[String]("i_time_dow_top_N_output_path")
        .action((x, c) => c.copy(i_time_dow_top_N_output_path = x))
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
                     data_14cols_1: String = "data/step_3/data_simplified",
                     data_14cols_2: String = "",
                     pluname2id_input_path: String = "",
                     item_frequency_day_input_path: String = "",
                     removed_suddenly_Tn_input: String = "3,0.9,3",
                     sale_drop_Tn_input: String = "10,0.9,3",
                     sale_up_Tn_input: String = "10,0.3,10",

                     i_removed_suddenly_output_path: String = "",
                     i_sale_up_output_path: String = "",
                     i_sale_drop_output_path: String = "",
                     i_necessities_output_path: String = "",
                     i_necessities_special_output_path: String = "",
                     i_expensive_necessities_output_path: String = "",
                     i_expensive_luxury_output_path: String = "",
                     i_coldest_output_path: String = "",
                     topn_entropy: Int = 10,
                     i_time_dow_top_N_output_path: String = "",
                     i_time_hour_top_N_output_path: String = "",
                     i_time_month_top_N_output_path: String = "",
                     i_time_season_top_N_output_path: String = "",
                     i_time_workday_top_N_output_path: String = ""
                   )

}
