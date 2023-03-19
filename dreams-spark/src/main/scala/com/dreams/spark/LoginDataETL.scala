package com.dreams.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.control.Breaks._

object LoginDataETL {

  def main(args: Array[String]): Unit = {
    val longIp = ip2Long("0.0.0.0")
    println(longIp)
  }


  def ip2Long(ip: String): Long ={
    val ele: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for(i <- 0 until ele.length){
      ipNum = ele(i).toLong | ipNum << 8L
    }
    ipNum
  }





}
