package com.xiaoi.datacenter.ods.study

import com.xiaoi.datacenter.base.PairRDDMultipleTextOutputFormat
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
  private val hdfsstudyaskpath : String = ConfigUtils.HDFS_STUDY_ASK_PATH
  private var studyDataInfo : RDD[String] = _


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
//        .config("hive.metastore.uris",hiveMetaStoreUris).enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext
      sc.setLogLevel("ERROR")
      studyDataInfo = sc.textFile(
        "D:\\data_for_datacenter\\01")
    }else{
      sparkSession = SparkSession.builder()
        .appName("ProduceClientLog")
        .enableHiveSupport()
        .getOrCreate()
      sc = sparkSession.sparkContext
      studyDataInfo = sc.textFile(s"${hdfsstudyaskpath}/currentday_clientlog.tar.gz")
    }


    val count: Long = studyDataInfo.count()
    println(s"count: ${count}")


    /**
     * Ask(
     * s(0), //          visit_time: String,
     * s(1), //          session_id: String,
     * s(2), //          user_id: String,
     * s(3), //          question: String,
     * s(4), //          question_type: String,
     * s(5), //          answer: String,
     * answerType, //    answer_type: String,
     * s(7), //          faq_id: String,
     * s(8), //          faq_name: String,
     * s(9), //          keyword: String,
     * s(10), //          city: String,
     * s(11), //          brand: String,
     * s(12), //          similarity: String,
     * s(13), //          module_id: String,
     * s(14), //          platform: String,
     * s(15), //          ex: String,
     * s(16) //          category: String,
     * )
     */
    val tableName = "ROBOT_ASK_DETAIL"
    studyDataInfo.map(line => line.split("\\|"))
      .filter(_.length > 16)
      .map(arr => {
        val visitTime: String = arr(0) // 访问时间
        val sessionId: String = arr(1) // Session id
        val userId: String = arr(2) // user id
        val question: String = arr(3) // 问句
        val questionType: String = arr(4) // 问句类型
        val answer: String = arr(5) // 回答
        val answerType: String = arr(6) // 回答类型
        val faqId: String = arr(7) // 标准问ID
        val faqName: String = arr(8) // 标准问
        val keyword: String  = arr(9) // 关键词
        val city: String = arr(10) // 服务对象
        val brand: String = arr(11) // 品牌
        val similarity: String = arr(12) // 问句和标准问的相似度
        val moduleId: String = arr(13) // 模块Id
        val platform: String = arr(14) // 平台
        val ex: String = arr(15)
        val categoryId: String = arr(16) //种类
        (tableName, visitTime + "\t" + sessionId + "\t" + userId + "\t" +
          question + "\t" + questionType + "\t" + answer + "\t" + answerType + "\t" +
          faqId + "\t" + faqName + "\t" + keyword + "\t" + city + "\t" +
          brand + "\t" + similarity + "\t" + moduleId + "\t" + platform + "\t" +
          ex + "\t" + categoryId)
      }).saveAsHadoopFile(s"${hdfsstudyaskpath}/all_study_ask_tables/${logDate}",
      classOf[String], classOf[String], classOf[PairRDDMultipleTextOutputFormat])

    /**
     * 在Hive中创建 ODS层的 TO_STUDY_ASK_D表
     */
    sparkSession.sql(s"use $hiveDataBase ")
    sparkSession.sql(
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS `TO_STUDY_ASK_D`(
        | `VISIT_TIME` string,  --访问时间
        | `SESSION_ID` string,     --会话ID
        | `USER_ID` string,  --用户ID
        | `QUESTION` string,     --问句
        | `QUESTION_TYPE` string,  --问句类型
        | `ANSWER` string,      --回答
        | `ANSWER_TYPE` string,    --回答类型
        | `FAQ_ID` string,      --标准问ID
        | `FAQ_NAME` string,        --标准问内容
        | `KEYWORD` string,       --关键词
        | `CITY` string,          --服务对象
        | `BRAND` string,         --品牌
        | `SIMILARITY` string,    --相似度
        | `MODULE_ID` string,     --模块ID
        | `PLATFORM` string,      --平台
        | `EX` string,
        | `CATEGORY_NAME`         --种类名称
        |)
        |partitioned by (data_dt string)
        |ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
        |LOCATION 'hdfs://192.168.43.26:9000/user/hive/warehouse/data/study/TO_STUDY_ASK_D'
      """.stripMargin
    )



    // 将数据从HDFS写到hive中
    sparkSession.sql(
      s"""
         | load data inpath
         | 'hdfs://192.168.43.26:9000/logdata/all_client_tables/${logDate}/ROBOT_ASK_DETAIL'
         | into table TO_STUDY_ASK_D partition (data_dt='${logDate}')
      """.stripMargin
    )

    println("**** all finished *** ")

  }





}
