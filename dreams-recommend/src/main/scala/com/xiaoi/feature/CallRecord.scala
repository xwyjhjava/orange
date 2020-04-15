package com.xiaoi.feature

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import scopt.OptionParser

/**
  * 通话记录表
  */
object CallRecord{

  def run(params: Params): Unit ={

    val spark = SparkSession.builder()
      .appName("call_record_etl")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.option("header", true).csv(params.call_record_input_path)

    df.select("CLT_NBR", "IS_EST")
      .withColumn("IS_EST", when(col("IS_EST"), "1"))


  }

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("callRecordETL") {
      opt[String]("call_record_input_path")
        .action((x, c) => c.copy(call_record_input_path = x))
      opt[String]("call_record_output_path")
        .action((x, c) => c.copy(call_record_output_path = x))
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
                     call_record_input_path: String = "",
                     call_record_output_path: String = ""
                   )



}
