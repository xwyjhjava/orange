package com.xiaoi.datacenter.ods.study

import com.xiaoi.datacenter.common.ConfigUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @Package com.xiaoi.datacenter.ods.study
 * @author ming
 * @date 2020/12/21 19:57
 * @version V1.0
 * @description 自学习的自动问答明细数据
 *             这个类的当前是手动写，预期后期是通过界面操作的形式完成
 *
 */
object ProduceStudyData {


  private val localrun: Boolean = ConfigUtils.LOCAL_RUN
  private val hiveMetaStoreUris = ConfigUtils.HIVE_METASTORE_URIS
  private val hiveDataBase = ConfigUtils.HIVE_DATABASE
  private var sparkSession : SparkSession = _
  private var sc: SparkContext = _
  private val hdfsclientlogpath : String = ConfigUtils.HDFS_CLIENT_LOG_PATH
  private var clientLogInfos : RDD[String] = _


  def main(args: Array[String]): Unit = {

    if(args.length<1){
      println(s"需要指定 数据日期")
      System.exit(1)
    }
    val logDate = args(0) // 日期格式 ： 年月日 20201231


    if(localrun){
      sparkSession = SparkSession.builder()
        .master("local")
        .appName("ProduceClientLog")
        .config("hive.metastore.uris",hiveMetaStoreUris).enableHiveSupport().getOrCreate()
      sc = sparkSession.sparkContext
      clientLogInfos = sc.textFile(
        "D:\\idea2019.3workspace\\MusicProject\\data\\currentday_clientlog.tar.gz")
    }else{
      sparkSession = SparkSession.builder()
        .appName("ProduceClientLog")
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext
      clientLogInfos = sc.textFile(s"${hdfsclientlogpath}/currentday_clientlog.tar.gz")
    }





  }





}
