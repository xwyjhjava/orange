package com.dreams.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * @Package com.dreams.spark.knowledge
 * @author ming
 * @date 2020/11/24 16:35
 * @version V1.0
 * @description TODO
 */
object LearnBaseRDD {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("api learn")
      .master("local[1]")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext

    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2: RDD[Int] = sc.parallelize(List(3, 4, 5, 6, 7))
    println("rdd1 partitions size == " + rdd1.partitions.size)
    println("rdd2 partitions size == " + rdd2.partitions.size)


    // ==================== union ========================
//    val unionRDD: RDD[Int] = rdd1.union(rdd2)
//    println(unionRDD.partitions.size)
//    unionRDD.foreach(println)


    //  ====================笛卡尔积 =======================
//    val cartesianRDD: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
//    cartesianRDD.foreach(println)

    // ====================交集===========================
    val intersectionRDD: RDD[Int] = rdd1.intersection(rdd2)
    intersectionRDD.foreach(println)


    // ==================== 差集=========================
    val subtractRDD: RDD[Int] = rdd1.subtract(rdd2)
    subtractRDD.foreach(println)


    while (true){

    }







  }

}
