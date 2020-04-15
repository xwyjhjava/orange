package com.xiaoi.feature

import com.xiaoi.spark.features.UserBasic.{Params, logger}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory
import scopt.OptionParser
import org.apache.spark.sql.functions._

object UserBasic {


  val logger = LoggerFactory.getLogger(getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)

  def run(params: Params): Unit ={
    val spark = SparkSession.builder()
      .appName("user_basic_feature")
      .master("local[*]")
      .getOrCreate()

    logger.info("user basic feature start")

    val userBasic = new UserBasic(spark, params)
    //预处理
    val df_user = userBasic.userEtl(spark, params)
    //特征处理
    userBasic.buildUserBasicFeature(df_user, spark, params)
  }


  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("basicFeature") {

      opt[String]("user_basic_train_output_path")
        .action((x, c) => c.copy(user_basic_train_output_path = x))
      opt[String]("user_basic_input_path")
        .action((x, c) => c.copy(user_basic_input_path = x))
      opt[String]("user_family_input_path")
        .action((x, c) => c.copy(user_family_input_path = x))
      opt[String]("user_communication_input_path")
        .action((x, c) => c.copy(user_communication_input_path = x))
      opt[String]("user_application_input_path")
        .action((x, c) => c.copy(user_application_input_path = x))
      opt[String]("user_output_path")
        .action((x, c) => c.copy(user_output_path = x))
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
                   user_basic_input_path: String = "tb_user.csv",
                   user_family_input_path: String = "tb_family.csv",
                   user_communication_input_path: String = "tb_contact",
                   user_application_input_path: String = "tb_application.csv",
                   user_basic_train_output_path: String = "",
                   user_output_path: String = ""
                   )

}

class UserBasic(private val spark: SparkSession, params: Params) extends Serializable {

  //  转换年龄
  def categorized_age(df: Dataset[Row]): Dataset[Row] ={

    val ageUdf = udf((age: String) => {
      age.toDouble match {
        case age if age >= 18 && age <= 30 => 1
        case age if age >= 31 && age <= 40 => 2
        case age if age >= 41 && age <= 50 => 3
        case age if age >= 51 && age <= 60 => 4
        case _ => 5
      }
    })

    df.withColumn("CLT_AGE", ageUdf(col("CLT_AGE")))
  }

  //转换收入
  def categorized_income(df: Dataset[Row]): Dataset[Row] ={
    val incomeUdf = udf((income: String) => {
      income.toDouble match {
        case income if income >= 0 && income <= 6 => 1
        case income if income > 6 && income <= 12 => 2
        case income if income > 12 && income <= 20 => 3
        case income if income > 20 && income <= 50 => 4
        case income if income > 50 => 5
        case _ => 6
      }
    })
    df.withColumn("CLT_INC", incomeUdf(col("CLT_INC")))
  }


  //预处理
  def userEtl(spark: SparkSession, params: Params): Dataset[Row] ={

    val df1 = spark.read.option("header",true).csv(params.user_basic_input_path)
    //从客户基础属性表中选择： 客户号,性别,年龄,国籍别,民族,行业类别,职业等级,职称,学历
    val user_part_one_df = df1.select("CLT_NBR","CLT_SEX", "CLT_AGE", "CLT_CTY_ID", "CLT_ETH_GRP", "CLT_COR_TYP",
      "CLT_CC_COD", "CLT_OC_COD", "CLT_EDU")
    //    +---------+-------+-------+----------+-----------+-----------+----------+----------+-------+
    //    |  CLT_NBR|CLT_SEX|CLT_AGE|CLT_CTY_ID|CLT_ETH_GRP|CLT_COR_TYP|CLT_CC_COD|CLT_OC_COD|CLT_EDU|
    //    +---------+-------+-------+----------+-----------+-----------+----------+----------+-------+
    //    |100052015|      1|     48|         1|       null|          4|        41|        11|     04|
    //    |119062268|      1|     41|         1|         01|       null|        60|        40|     00|
    //    +---------+-------+-------+----------+-----------+-----------+----------+----------+-------+
    //    user_part_one_df.show()
    //从家庭属性和通讯属性表中选择：客户号,婚姻状况,有无子女
    val df2 = spark.read.option("header", true).csv(params.user_family_input_path)
    val user_part_two_df = df2.select("CLT_NBR", "CLT_MAR_STS", "CLT_CHILD_FLG")
    //    +---------+-----------+-------------+
    //    |  CLT_NBR|CLT_MAR_STS|CLT_CHILD_FLG|
    //    +---------+-----------+-------------+
    //    |100072706|          0|         null|
    //    |119062268|          2|            1|
    //    |100207805|          0|         null|
    //    +---------+-----------+-------------+
    //    user_part_two_df.show()
    //从通讯联系属性和财产属性表中选择：客户号,收入,车辆状况,现住房屋状况,AI进件身份
    val df3 = spark.read.option("header", true).csv(params.user_communication_input_path)
    val user_part_three_df = df3.select("CLT_NBR", "CLT_INC", "VEH_FLG", "CLT_HOS_STS", "CLT_APP_INC_COD")
    //    +---------+-------+-------+-----------+---------------+
    //    |  CLT_NBR|CLT_INC|VEH_FLG|CLT_HOS_STS|CLT_APP_INC_COD|
    //    +---------+-------+-------+-----------+---------------+
    //    |119062268| 100000|      N|          0|             01|
    //    |119078738|  35000|      N|          5|             35|
    //    +---------+-------+-------+-----------+---------------+
    //    user_part_three_df.show()
    //从信用卡申请书属性表中选择
    val df4 = spark.read.option("header", true).csv(params.user_application_input_path)
    val user_part_four_df = df4.select("CLT_NBR","CLT_CLA", "APP_DTE")
    //    user_part_four_df.show()

    val user_basic_df = user_part_one_df.join(user_part_two_df, "CLT_NBR")
      .join(user_part_three_df, "CLT_NBR")

    //    +---------+-------+-------+----------+-----------+-----------+----------+----------+-------+-----------+-------------+-------+-------+-----------+---------------+
    //    |  CLT_NBR|CLT_SEX|CLT_AGE|CLT_CTY_ID|CLT_ETH_GRP|CLT_COR_TYP|CLT_CC_COD|CLT_OC_COD|CLT_EDU|CLT_MAR_STS|CLT_CHILD_FLG|CLT_INC|VEH_FLG|CLT_HOS_STS|CLT_APP_INC_COD|
    //    +---------+-------+-------+----------+-----------+-----------+----------+----------+-------+-----------+-------------+-------+-------+-----------+---------------+
    //    |119062268|      1|     41|         1|         01|       null|        60|        40|     00|          2|            1| 100000|      N|          0|             01|
    //    +---------+-------+-------+----------+-----------+-----------+----------+----------+-------+-----------+-------------+-------+-------+-----------+---------------+
    //    user_basic_df.show()

    user_basic_df
  }

  //构建客户基本特征
  def buildUserBasicFeature(df_user: Dataset[Row], spark: SparkSession, params: Params): Unit ={

    val df2 = categorized_age(df_user)
    val df3 = categorized_income(df2)

    logger.info("save train csv")

    df3.coalesce(1).write.option("header", false)
      .csv(params.user_basic_train_output_path)
  }

}
