package com.xiaoi.feature

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Bucketizer, OneHotEncoder, StringIndexer}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CreditCard {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val creditCard = new CreditCard(spark)

    val path_card = "tb_card.csv"
    val path_consume_point = "tb_consume_point.csv"
    val path_account = "tb_account.csv"
    val path_consume_record = "tb_consume_record.csv"
    val path_stage_detail = "tb_stage_detail.csv"
    val path_sign = "tb_sign.csv"
    val path_stat_month = "tb_statistic_month.csv"

    val df_account = creditCard.process_account(path_account)
    val df_card = creditCard.process_card(path_card)
    //    val df_consume_point = creditCard.process_consume_point(path_consume_point)
    val df_consume_record = creditCard.process_consume_record(path_consume_record)
    val df_stat_month = creditCard.process_stat_month(path_stat_month)
    val df_sign = creditCard.process_sign(path_sign)
    val df_stage_detail = creditCard.process_stage_detail(path_stage_detail)


    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    val df_creditCard = df_account
      .join(df_card, "CLT_NBR")
      .join(df_consume_record, "CLT_NBR")
      .join(df_stat_month, "CLT_NBR")
      .join(df_sign, "CLT_NBR")
      .join(df_stage_detail, "CLT_NBR")

    df_creditCard.write.parquet("./credit_card.parquet")
  }

}

class CreditCard(private val spark: SparkSession) extends Serializable {
  import org.apache.spark.sql.functions._
  import spark.implicits._

  val path_card = "tb_card.csv"
  val path_consume_point = "tb_consume_point.csv"
  val path_account = "tb_account.csv"
  val path_consume_record = "tb_consume_record.csv"
  val path_stage_detail = "tb_stage_detail.csv"
  val path_sign = "tb_sign.csv"
  val path_stat_month = "tb_statistic_month.csv"

  def read_csv(path: String)={
    spark.read.option("header", "true").csv(path)
  }

  //根据传入的Array对列进行分箱处理，然后删除原列
  def BucketWithList(df: Dataset[Row], colName: String, arr: Array[Double])={
    val df1 = df.withColumn(colName,col(colName).cast(DataTypes.DoubleType))
    val valueBucketer = new Bucketizer()
      .setSplits(arr)
      .setInputCol(colName)
      .setOutputCol(colName + "_cate")
    valueBucketer.transform(df1).drop(colName)
  }

  //+---------+-------+--------------------+-----------+-------+-----------+-----------+-------+-----------+-----------+
  //|  CLT_NBR|PER_CRL|             DCQ_HST|ACT_STR_DTE|CRD_LNT|CIR_CRD_CNT|ATV_CRD_CNT|AVL_CRL|AVL_CSH_CRL|LST_TRX_DTE|
  //+---------+-------+--------------------+-----------+-------+-----------+-----------+-------+-----------+-----------+
  //|100099615|  39000|BBBBBBBBBBBBBBBBB...| 2003-05-20|      5|          2|          4|  25093|    19500.0| 2019-09-28|
  //+---------+-------+--------------------+-----------+-------+-----------+-----------+-------+-----------+-----------+
  def process_account(path: String) = {
    val df_account = read_csv(path)
      .select("CLT_NBR","PER_CRL","DCQ_HST",
        "ACT_STR_DTE","CRD_LNT","CIR_CRD_CNT","ATV_CRD_CNT",
        "AVL_CRL","AVL_CSH_CRL","LST_TRX_DTE")
//        .select(UDF.addColumn(lit(date)).as("CREATE_DATE"))
      .withColumn("ACT_STR_DTE", UDF.getDiffYears($"ACT_STR_DTE", lit(DateUtil.getDate())))
      .withColumn("AVL_CSH_CRL", UDF.getDiffDateTime($"AVL_CSH_CRL", lit(DateUtil.getDate())))

    //永久信用额度分箱
    val crl_borders = 0.0 +: Array(1000.0, 10000.0, 50000, 100000) :+ 300000.0
    val df1 = BucketWithList(df_account, "PER_CRL", crl_borders)
    val df2 = df1.withColumn("DCQ_HST", categorize_DCQ_HST($"DCQ_HST"))
    df2
  }

  //对24个月催款记录进行分类,目前只做了逾期和未逾期(包含>1的字符)
  //B：全款 1:未全额  Z、0：未消费过 2：逾期一个月 3：逾期两个月 4：逾期三个月
  //如果是大于1的数字，表示逾期，逾期月数为标记数字减一
  val categorize_DCQ_HST = udf((field: String) => {
    if(field.matches(""".*[2-9].*""")) 1 else  0
  })



  //+---------+-------+-------+-----------+---------------+-------+
  //|  CLT_NBR|CRD_TRY|CRD_STS|CRD_CIR_FLG|CRD_WAV_AFF_FLG|CRD_AFE|
  //+---------+-------+-------+-----------+---------------+-------+
  //|164320086|    800|      2|          N|              0|    0.0|
  //|171247932|    800|      1|          Y|              0|    0.0|
  //+---------+-------+-------+-----------+---------------+-------+
  def process_card(path: String) = {
    val df_card = read_csv(path)
      .select("CLT_NBR","CRD_TRY","AFF_COD",
        "CRD_STS","CRD_CIR_FLG","CRD_WAV_AFF_FLG","CRD_AFE")
      ////招行内部通过CRD_TRY+AFF_COD两个字段组合判断出卡类型
      .withColumn("CRD_TYPE", $"CRD_TRY" + $"AFF_COD")
    //todo 年费需要分箱处理
    df_card
  }

  //|  CLT_NBR|CUR_BBNS_VAL|CUR_EBNS_VAL|CUR_ADUP_BNS_VAL|CUR_ADDN_BUS_VAL|CUR_USED_BNS_VAL|AVL_BNS_BAL|FRZ_BNS_BAL|LST_BNS_BAL|BNS_EXP_DTE|
  //+---------+------------+------------+----------------+----------------+----------------+-----------+-----------+-----------+-----------+
  //|100011287|           0|           0|               0|               0|               0|      33211|          0|      33211| 2017-12-31|
  //|100012195|          15|           0|              20|               0|               0|       1301|          0|       1266| 2017-12-31|
  //积分变化表在电销行为之后，所以暂不使用
  def process_consume_point(path: String) = {
    read_csv(path)
  }


  //|  CLT_NBR|         ACT_NBR|CCY_COD|MCC_COD|STM_MON_DTE|TRS_TYP|TRS_AMT|TRS_CTY|                  TRS_DES|
  //+---------+----------------+-------+-------+-----------+-------+-------+-------+-------------------------+
  //|100495764|0100495764001001|    156|      0| 2019-07-01|      1|   22.0|     CN|美团支付-美团点评外卖商户|
  //|100510180|0100510180001001|    156|      0| 2019-04-01|      1|   13.0|     CN|  生活缴费-宁波市海曙电费|
  def process_consume_record(path: String) = {
    read_csv(path)
      .select("CLT_NBR","ACT_NBR","CCY_COD","MCC_COD",
        "STM_MON_DTE","TRS_TYP","TRS_AMT","TRS_CTY","TRS_DES")
      .groupBy("CLT_NBR")
      .agg(collect_list("ACT_NBR").as("ACT_NBR_s"),
        count("*").as("CNT"))
      .drop("ACT_NBR")

  }




  //|  CLT_NBR|ZZY_LST_TRX_DTE|ZZY_LST_TRX_AMT|IS_ZFB_BIND|IS_WCHT_BIND|WCHT_BIND_CNT|REC_M6_CNT|
  //+---------+---------------+---------------+-----------+------------+-------------+----------+
  //|100024874|     1900-01-01|            0.0|          N|           Y|            1|         0|
  //| 10008811|     1900-01-01|            0.0|          N|           Y|         null|         N|
  def process_sign(path: String) = {
    val df_sign = read_csv(path)
      .select("CLT_NBR","ZZY_LST_TRX_DTE","ZZY_LST_TRX_AMT",
        "IS_ZFB_BIND","IS_WCHT_BIND","WCHT_BIND_CNT","REC_M6_CNT")
      //最近购买朝朝盈到现在的年数
      .withColumn("ZZY_LST_TRX_DTE", UDF.getDiffYears($"ZZY_LST_TRX_DTE", lit(DateUtil.getDate())))
    //最近购买朝朝盈的金额
    val last_amt = Array().asInstanceOf[Array[Double]]
    val df1 = BucketWithList(df_sign, "ZZY_LST_TRX_AMT",last_amt)
    df1
  }

  //|  CLT_NBR|TOT_IST_CNT|CLT_FEE|FST_AMT|TOT_CLT_AMT|TOT_CLT_PNT|
  //+---------+-----------+-------+-------+-----------+-----------+
  //|152849824|          2|   20.0|  510.0|     1000.0|          0|
  //|151336770|         12| 133.41| 151.49|     168.44|          0|
  def process_stage_detail(path: String) = {
    val df_stage = read_csv(path)
      .select("CLT_NBR","TOT_IST_CNT","CLT_FEE",
        "FST_AMT","TOT_CLT_AMT","TOT_CLT_PNT")
    //总分期数分箱处理 - todo 需要知道分期数量的分布情况
    val tot_ist_cnt = Array().asInstanceOf[Array[Double]]
    val df1 = BucketWithList(df_stage, "TOT_IST_CNT", tot_ist_cnt)

    //卡人手续费 todo 需要知道卡人手续费的分布情况
    val clt_fee = Array().asInstanceOf[Array[Double]]
    val df2 = BucketWithList(df1, "CLT_FEE", clt_fee)

    //首期金额 todo 首期金额
    val fst_amt = Array().asInstanceOf[Array[Double]]
    val df3 = BucketWithList(df2, "FST_AMT", fst_amt)

    //卡人消费总金额  todo 分布情况
    val tot_clt_amt = Array().asInstanceOf[Array[Double]]
    val df4 = BucketWithList(df3, "TOT_CLT_AMT", tot_clt_amt)

    //卡人消费总积分
    val tot_clt_pnt = Array().asInstanceOf[Array[Double]]
    val df5 = BucketWithList(df4, "TOT_CLT_PNT", tot_clt_pnt)
    df5
  }

  //todo 数据没有示例
  def process_stat_month(path: String) ={
    read_csv(path)
      .select("CLT_NBR","STM_MON_DTE","TRS_COD",
        "MCC_TYPE1","TRS_CN","TRS_AMT")
  }

  def index_onehot(df: Dataset[Row]): Tuple2[Dataset[Row], Array[String]] = {
    //account
    //可用信用额度分箱  "AVL_CRL"
    //临时信用额度分箱  "TMP_CRL"
    //可用取现额度分箱  "AVL_CSH_CRL"
    //card
    //卡片类型分类 "CRD_TYPE"
    //卡片流通状态  "CRD_CIR_FLG"
    //免除年费标志 "CRD_WAV_AFF_FLG"
    //sign
    //是否绑定微信  "IS_WCHT_BIND"
    //是够绑定支付宝  "IS_ZFB_BIND"

    val stringCols = Array("AVL_CRL","TMP_CRL", "AVL_CSH_CRL",
      "CRD_TYPE", "CRD_CIR_FLG", "CRD_WAV_AFF_FLG",
      "IS_WCHT_BIND","IS_ZFB_BIND")
    val subOneHotCols = stringCols.map(cname => s"${cname}_index")
    val index_transformers: Array[org.apache.spark.ml.PipelineStage] = stringCols.map(
      cname => new StringIndexer()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_index")
        .setHandleInvalid("skip")
    )


    val oneHotCols = subOneHotCols ++ Array("Pclass", "Age_categorized", "Fare_categorized", "family_type")
    val vectorCols = oneHotCols.map(cname => s"${cname}_encoded")
    val encode_transformers: Array[org.apache.spark.ml.PipelineStage] = oneHotCols.map(
      cname => new OneHotEncoder()
        .setInputCol(cname)
        .setOutputCol(s"${cname}_encoded")
    )

    val pipelineStage = index_transformers ++ encode_transformers
    val index_onehot_pipeline = new Pipeline().setStages(pipelineStage)
    val index_onehot_pipelineModel = index_onehot_pipeline.fit(df)

    val resDF = index_onehot_pipelineModel.transform(df).drop(stringCols:_*).drop(subOneHotCols:_*)
    println(resDF.columns.size)
    (resDF, vectorCols)
  }

}

