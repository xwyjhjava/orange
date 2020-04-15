package com.xiaoi.feature

import com.xiaoi.spark.features.Consume.Params
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory
import scopt.OptionParser
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

/**
  *
  * 分析客户的消费特征
  *
  */

object Consume {


  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit ={

    logger.info("consume feature start")

    val spark = SparkSession.builder()
      .appName("consume_feature")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._
    val df = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv(params.consume_input_path)

    //客户号, 账户, 分期期数, 交易日, 交易类型, TCCOD, 交易金额, 交易码
    val base_df = df.select("CLT_NBR", "ACT_NBR", "IST_IND", "TRS_DTE", "TRS_TYP", "MCH_NBR", "TC_COD", "TRS_AMT", "TRS_COD")
      //只处理个人账户
      .filter($"ACT_NBR".contains("001001"))

    val utils = new Consume(spark, params)

    val df1_1 = utils.process_month(df,1)
    val df1_2 = utils.process_month(df, 3)
    val df1_3 = utils.process_month(df, 6)

    df1_1.join(df1_2, "CLT_NBR")
      .join(df1_3, "CLT_NBR")
      .drop("CLR_NBR")
      .write
      .parquet(params.consume_output_path)
    

    val df2_1 = utils.process_returned(df, 1)
    val df2_2 = utils.process_returned(df, 3)
    val df2_3 = utils.process_returned(df, 6)

    val df3_1 = utils.process_repayment(df, 1)
    val df3_2 = utils.process_repayment(df, 3)
    val df3_3 = utils.process_repayment(df, 6)

    val df4_1 = utils.process_stage(df, 1)
    val df4_2 = utils.process_stage(df, 3)
    val df4_3 = utils.process_stage(df, 6)

    val df5_1 = utils.process_withdraw(df, 1)
    val df5_2 = utils.process_withdraw(df, 3)
    val df5_3 = utils.process_withdraw(df, 6)

    val df6_1 = utils.process_breach(df, 1)
    val df6_2 = utils.process_breach(df, 3)
    val df6_3 = utils.process_breach(df, 6)

    val df7_1 = utils.process_feedback(df, 1)
    val df7_2 = utils.process_feedback(df, 3)
    val df7_3 = utils.process_feedback(df, 6)



  }




  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("consumeFeature"){
      opt[String]("consume_input_path")
        .action((x, c) => c.copy(consume_input_path = x))
      opt[String]("consume_output_path")
        .action((x, c) => c.copy(consume_output_path = x))
      opt[String]("target_start_date")
        .action((x, c) => c.copy(target_start_date = x))
      opt[String]("target_end_date")
        .action((x, c) => c.copy(target_end_date = x))
      opt[Double]("threshold_amount")
        .action((x, c) => c.copy(threshold_amount = x))
      checkConfig{ params =>
        success
      }
    }
    parser.parse(args, defaultParams).map{params =>
      run(params)
    }.getOrElse{
      sys.exit(1)
    }
  }



  case class Params(
                     consume_input_path: String = "",
                     consume_output_path: String = "",
                     target_start_date: String = "",
                     target_end_date: String = "2019-11-07",
                     threshold_amount: Double = 10000.0
                   )
}


class Consume(private val spark: SparkSession, param: Params){

  import spark.implicits._
  //是否分期
  def process_stage(df: Dataset[Row]): Unit ={

    val stageUdf = udf((stage: String) => {
      if(stage.toInt == 0) 0
      else 1
    })

    df.withColumn("IS_STAGE", stageUdf($"IST_IND"))
  }

  //RFM, 只看消费, 即TRS_CODE是4000的消费
  //一个月/三个月/六个月/十二个月
  def process_month(df: Dataset[Row], month: Int): Dataset[Row] ={

    val before_date = new BaseFun().getDateBefore(month)

    val df1 = df
      .filter($"TC_COD" === 4000)
      .filter($"TRS_DTE" >= before_date)
      .withColumn("TRS_AMT", col("TRS_AMT").cast(DataTypes.DoubleType))
      .groupBy("CLT_NBR")
      .agg(
        //最近消费日期
        max("TRS_DTE").as("TRS_DTE_MAX"),
        //消费次数
        count("TC_COD").as("frequency_count"),
        //消费金额
        sum("TRS_AMT").as("amt_sum"),
        //单笔最大消费金额
        max("TRS_AMT").as("amt_max"),
        //不同种商户类型数
        countDistinct("MCH_NBR"))

    val df2 = recent_grade(df1)
    val df3 = frequency_grade(df2, month, "frequency_grade", "frequency_count")
    val df4 = amount_grade(df3, month, "amt_grade", "amt_sum")
    df4
  }



  //最近消费时间距离现在的天数分箱
  def recent_grade(df: Dataset[Row]): Dataset[Row] ={

    val recentUdf = udf((date: String) => {
      val interval = new BaseFun().daysBetweenNow(date)
      interval match {
        case interval if interval > 0 && interval <= 30 => 1
        case interval if interval > 30 && interval <= 60 => 2
        case interval if interval > 60 && interval <= 90 => 3
        case interval if interval > 90 && interval <= 180 => 4
        case interval if interval > 180 => 5
        case _ => 0
      }
    })
    df.withColumn("recent_grade", recentUdf($"TRS_DTE_MAX"))
      .drop("TRS_DTE_MAX")
  }


  //消费次数分箱
  def frequency_grade(df: Dataset[Row], month: Int, newCol: String, oldCol: String): Dataset[Row] ={

    val frequencyUdf = udf((frequency: Int) => {

      frequency match {
        case frequency if frequency >= 0 && frequency < 2 => 5
        case frequency if frequency >= 2 && frequency < 5 => 4
        case frequency if frequency >= 5 && frequency < 11 => 3
        case frequency if frequency >= 11 && frequency < 21 => 2
        case frequency if frequency >= 21 => 1
        case _ => 0
      }
    })
    df.withColumn(newCol, frequencyUdf(col(oldCol)/month))
      .drop(oldCol)
  }


  //消费金额分箱, 以一个月作为基准
  def amount_grade(df: Dataset[Row], month: Int, newCol: String, oldCol: String): Dataset[Row] ={

    val amountUdf = udf((avg_month_amount: Double) => {

      avg_month_amount match {
        case avg_month_amount if avg_month_amount <= 300 => 5
        case avg_month_amount if avg_month_amount >300 && avg_month_amount <= 1000 => 4
        case avg_month_amount if avg_month_amount > 1000 && avg_month_amount <= 5000 => 3
        case avg_month_amount if avg_month_amount > 5000 && avg_month_amount <= 10000 => 2
        case avg_month_amount if avg_month_amount > 10000 => 1
        case _ => 0
      }
    })
    df.withColumn(newCol, amountUdf(col(oldCol)/month))
      .drop(oldCol)
  }




  //退货（退款）
  def process_returned(df: Dataset[Row], month: Int): Dataset[Row] ={

    val before_date = new BaseFun().getDateBefore(month)

    val df1 = df.filter($"TC_COD" === 4100)
      .filter($"TRS_DTE" >= before_date)
      .groupBy("CLT_NBR")
      .agg(
        //退货次数
        count("TC_COD").as("return_count"),
        //退货金额
        sum(abs($"TRS_AMT")).as("return_amt_sum")
      )

    val df2 = frequency_grade(df1, month, "return_count_grade", "return_count")
    val df3 = amount_grade(df2, month, "return_amt_grade", "return_amt_sum")

    df3.select("CLT_NBR", "return_count_grade", "return_amt_grade")
  }


  //还款
  def process_repayment(df: Dataset[Row], month: Int): Dataset[Row] ={

    val before_date = new BaseFun().getDateBefore(month)

    val df1 = df.filter($"TC_COD" === 2000)
      .filter($"TRS_DTE" >= before_date)
      .groupBy("CLT_NBR")
      .agg(
        //还款次数
        count("TC_COD").as("repay_count"),
        //还款金额
        sum(abs($"TRS_AMT")).as("repay_amt_sum")
      )

    val df2 = frequency_grade(df1, month, "repay_count_grade", "repay_count")
    val df3 = amount_grade(df2, month, "repay_amt_grade", "repay_amt_sum")

    df3.select("CLT_NBR", "repay_count_grade", "repay_amt_grade")
  }

  //分期
  def process_stage(df: Dataset[Row], month: Int): Dataset[Row] ={

    val before_date = new BaseFun().getDateBefore(month)

    val df1 = df.filter($"TC_COD" === 4300)
      .filter($"TRS_DTE" >= before_date)
      .groupBy("CLT_NBR")
      .agg(
        //分期次数
        count("TC_COD").as("stage_count"),
        //分期金额
        sum(abs($"TRS_AMT")).as("stage_amt_sum")
      )

    val df2 = frequency_grade(df1, month, "stage_count_grade", "stage_count")
    val df3 = amount_grade(df2, month, "stage_amt_grade", "stage_amt_sum")

    df3.select("CLT_NBR", "stage_count_grade", "stage_amt_grade")
  }

  //预借现金(提现)
  def process_withdraw(df: Dataset[Row], month: Int): Dataset[Row] ={

    val before_date = new BaseFun().getDateBefore(month)

    val df1 = df.filter($"TC_COD" === 3000)
      .filter($"TRS_DTE" >= before_date)
      .groupBy("CLT_NBR")
      .agg(
        //提现次数
        count("TC_COD").as("withdraw_count"),
        //提现金额
        sum("TRS_AMT").as("withdraw_amt_sum")
      )
    val df2 = frequency_grade(df1, month, "withdraw_count_grade", "withdraw_count")
    val df3 = amount_grade(df2, month, "withdraw_amt_grade", "withdraw_amt_sum")

    df3.select("CLT_NBR", "withdraw_count_grade", "withdraw_amt_grade")
  }

  //违约金
  def process_breach(df: Dataset[Row], month: Int): Dataset[Row] ={

    val before_date = new BaseFun().getDateBefore(month)

    val df1 = df.filter($"TC_COD" === 6000)
      .filter($"TRS_DTE" >= before_date)
      .groupBy("CLT_NBR")
      .agg(
        //违约次数
        count("TC_COD").as("breach_count"),
        //违约金额
        sum("TRS_AMT").as("breach_amt_sum")
      )

    val df2 = frequency_grade(df1, month, "breach_count_grade", "breach_count")
    val df3 = amount_grade(df2, month, "breach_amt_grade", "breach_amt_sum")

    df3.select("CLT_NBR", "breach_count_grade", "breach_amt_grade")
  }

  //回馈金
  def process_feedback(df: Dataset[Row], month: Int): Dataset[Row] ={

    val before_date = new BaseFun().getDateBefore(month)

    val df1 = df.filter($"TC_COD" === 6000)
      .filter($"TRS_DTE" >= before_date)
      .groupBy("CLT_NBR")
      .agg(
        //回馈次数
        count("TC_COD").as("feedback_count"),
        //回馈金额
        sum("TRS_AMT").as("feedback_amt_sum")
      )

    val df2 = frequency_grade(df1, month, "feedback_count_grade", "feedback_count")
    val df3 = amount_grade(df2, month, "feedback_amt_grade", "feedback_amt_sum")

    df3.select("CLT_NBR", "feedback_count_grade", "feedback_amt_grade")
  }

}

