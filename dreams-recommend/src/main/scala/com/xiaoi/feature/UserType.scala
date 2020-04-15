package com.xiaoi.feature

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scopt.OptionParser
import org.slf4j.LoggerFactory
import org.apache.log4j.{Level, Logger}

/**
  * 名单类型
  *
  */
object UserType{


  def run(params: Params): Unit ={

    val logger = LoggerFactory.getLogger(getClass)
    Logger.getLogger("org").setLevel(Level.ERROR)


    logger.info("user type feature start")

    val spark = SparkSession.builder()
      .appName("userType feature")
      .master("local[6]")
      .getOrCreate()

    val df  = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(params.user_type_input_path)


    val utils = new UserType()
    val df1 = utils.processed_userType(df)

    logger.info("save user type")
//    df1.write.parquet(params.user_type_output_path)

  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("userTypeFeature"){
      opt[String]("user_type_input_path")
        .action((x, c) => c.copy(user_type_input_path = x))
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
                     user_type_input_path: String = "",
                     user_type_output_path: String = ""
                   )

}

class UserType extends Serializable {
  //处理名单类型
  def processed_userType(df: Dataset[Row]): Dataset[Row] ={
    new BaseFun().labelEncode(df, Array(""))
  }

}
