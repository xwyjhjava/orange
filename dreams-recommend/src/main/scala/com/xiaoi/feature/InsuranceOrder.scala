package com.xiaoi.feature

import com.xiaoi.spark.features.InsuranceOrder.Params
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.joda.time.{DateTime, Days}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.apache.spark.sql.functions._
import scopt.OptionParser

object InsuranceOrder {



  def run(params: Params): Unit ={


    val spark = SparkSession.builder()
      .appName("consume_feature")
      .master("local[6]")
      .getOrCreate()

    val df = spark.read.option("header", true).csv(params.insurance_order_input_path)
      .select("IPLCY_CUST_NO", "IPLCY_TSR_SALE_DATE", "IPLCY_TYPE", "IPLCY_COVERAGE", "IPLCY_INSURE_TERM_TY",
        "IPLCY_INSURE_TERM", "IPLCY_METHOD_TERM_TY", "IPLCY_METHOD_TERM", "IPLCY_PAYT_PREQ", "IPLCY_PERM_FEE",
        "IPLCY_YEAR_FEE", "IPLCY_TOT_FEE", "IPLCY_PAYT_AMT", "IPLCY_PAYED_PERM", "PAYER_INSURED_SAME_F", "INSURED_AGE")


    val insurance = new InsuranceOrder(spark, params)

    val df1 = insurance.process_sale_date(df)
    val df2 = insurance.process_insure_term(df1)
    val df3 = insurance.process_method_term(df2)
    val df4 = insurance.process_pay_type(df3)
    val df5 = insurance.process_term_fee(df4)
    val df6 = insurance.process_relationship(df5)
    val df7 = insurance.process_insured_age(df6)

    df7.withColumn("COVERAGE", col("IPLCY_COVERAGE").cast(DataTypes.IntegerType))
      .drop("IPLCY_COVERAGE", "IPLCY_YEAR_FEE", "IPLCY_TOT_FEE")
    //todo 累计已缴保费金额     已缴保费期数

    df7.show()
  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("insurance_order_feature") {

      opt[String]("insurance_order_input_path")
        .action((x, c) => c.copy(insurance_order_input_path = x))
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
                   insurance_order_input_path: String = "/home/ming/IdeaProjects/recommend/insurance/data/origin/tb_insurance_order.csv"
                   )

}

class InsuranceOrder(spark: SparkSession , params: Params) extends  Serializable {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  //电话营销时间, 距离现在的天数
  def process_sale_date(df: Dataset[Row]): Dataset[Row] ={

    val intervalUdf = udf((date: String) => {
      //      val datetime = fmt.parseDateTime(date)
      //      val now = DateTime.now()
      new BaseFun().daysBetweenNow(date, "YYYYMMdd")
    })
    df.withColumn("TSR_SALE_DATE_INTERVAL_DAYS", intervalUdf(col("IPLCY_TSR_SALE_DATE")))
      .drop("IPLCY_TSR_SALE_DATE")
  }


  //保险时长,统一转换成月数
  def process_insure_term(df: Dataset[Row]): Dataset[Row] ={

    val termUdf = udf((term_unit: String, term: String) => {
      var months = 0
      //数值单位是年
      if(term_unit == "01") months = term.toInt * 12
      //数值单位是月
      else if(term_unit == "02") months = term.toInt
      //数值单位是日
      else months = (term.toDouble / 30).toInt
      months
    })
    df.withColumn("INSURE_TERM", termUdf($"IPLCY_INSURE_TERM_TY", $"IPLCY_INSURE_TERM"))
      .drop("IPLCY_INSURE_TERM_TY", "IPLCY_INSURE_TERM")
  }

  //缴费时长, 统一转换成月数(保险总期数)
  def process_method_term(df: Dataset[Row]): Dataset[Row] ={

    val termUdf = udf((term_unit: String, term: String) => {

      var months = 0
      //数值单位是年
      if(term_unit == "01") months = term.toInt * 12
      //数值单位是月
      else if(term_unit == "02") months = term.toInt
      //数值单位是日
      else months = (term.toDouble / 30).toInt
      months
    })
    df.withColumn("METHOD_TERM", termUdf($"IPLCY_METHOD_TERM_TY", $"IPLCY_METHOD_TERM"))
      .drop("IPLCY_METHOD_TERM_TY", "IPLCY_METHOD_TERM")
  }

  //缴别
  def process_pay_type(df: Dataset[Row]): Dataset[Row] ={
    df.withColumn("PAYT_PREQ", col("IPLCY_PAYT_PREQ").cast(DataTypes.IntegerType))
      .drop("IPLCY_PAYT_PREQ")
  }

  //每期保费, 和本年度保费金额,总保费有重合的地方
  //本年度保费金额 = 每期保费 * 12
  //总保费 = 每期保费 * 总期数
  def process_term_fee(df: Dataset[Row]): Dataset[Row] ={

    val feeUdf = udf((fee: String) => {

      val fee_type = fee.toDouble match {
        case fee if fee > 0 && fee <= 100 => 1
        case fee if fee > 100 && fee <= 500 => 2
        case fee if fee > 500 && fee <= 1000 => 3
        case fee if fee > 1000 => 4
        case _ => 5
      }
      fee_type
    })
    df.withColumn("PERM_FEE", feeUdf(col("IPLCY_PERM_FEE")))
      .drop("IPLCY_PERM_FEE")
  }

  //被保人和投保人关系
  def process_relationship(df: Dataset[Row]): Dataset[Row] ={

    val relationUdf = udf((relation: String) => {
      var relation_type = 0
      if(relation == "两者相同") relation_type = 1
      relation_type
    })

    df.withColumn("PAYER_INSURED_SAME_F", relationUdf(col("PAYER_INSURED_SAME_F")))
  }

  //被保人年龄
  def process_insured_age(df: Dataset[Row]): Dataset[Row] ={

    val ageUdf = udf((age: String) => {
      age.toDouble match {
        case age if age >= 18 && age <= 30 => 1
        case age if age >= 31 && age <= 40 => 2
        case age if age >= 41 && age <= 50 => 3
        case age if age >= 51 && age <= 60 => 4
        case _ => 5
      }
    })

    df.withColumn("INSURED_AGE", ageUdf(col("INSURED_AGE")))

  }


}
