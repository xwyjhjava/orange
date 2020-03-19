package com.dreams.spark.knowledge

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @Package com.dreams.spark.knowledge
 * @author ming
 * @date 2020/3/18 10:04
 * @version V1.0
 * @description 解决数据倾斜
 */
class ResolveDataSkew(private val spark: SparkSession) extends Serializable {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  // join算子的数据倾斜
  def joinDataSkewToDeal(df1: DataFrame, df2: DataFrame, skewKey: String): Long ={

    // 从df1 中分离出 倾斜key的df
    val df1_1: Dataset[Row] = df1.filter($"key" === "A")
//    df1_1.show()
    // df1中不包含 倾斜key的df
//    println("========================")
    val df1_2: Dataset[Row] = df1.except(df1_1)
//    df1_2.show()

    //同理， 从df2中分离出包含倾斜key和不包含倾斜key的两类
//    println("========================")
    val df2_1: Dataset[Row] = df2.filter($"key" === "A")
//    df2_1.show()

//    println("========================")
    val df2_2: Dataset[Row] = df2.except(df2_1)
//    df2_2.show()


//    println("=======================df1_1_randomKey")
    //df1_1 包含倾斜key的df加随机前缀, 生成新的df
    val df1_1_randomKey: DataFrame = df1_1.map(row => {
      val key: String = row.getAs[String]("key")
      val value: String = row.getAs[String]("value1")
      // 生成随机数
      val random = new Random()
      val randomInt: Int = random.nextInt(2)
      val new_key: String = key + "_" + randomInt
      (new_key, value)
    }).toDF("key", "value1")

//    df1_1_randomKey.show(10)

//    println("====================df2_1_expand")


    //扩容df2-1 N倍， N为df1_1中随机前缀区间的最大值
    // TODO: 用ListBuffer实现了扩容， 缺点是消耗内存，可待更优方式
    val listBuffer = new ListBuffer[(String, String)]()
    val df2_1_expand: DataFrame = df2_1.flatMap(row => {
      val key: String = row.getAs[String]("key")
      val value: String = row.getAs[String]("value2")

      val random = new Random()
      for (x <- 1 to 2) {
        listBuffer += ((key + "_" + random.nextInt(2), value))
      }
      listBuffer.toList
    }).toDF("key", "value2")

//    df2_1_expand.show(10)

//    println(" ========================= join1")

    // df1_1_randomKey 和 df2_1_expand 相join
    val join1: DataFrame = df1_1_randomKey.join(df2_1_expand, "key")

//    join1.show(10)

//    println("=============================join1_without_prefix")

    // 去掉join1的随机前缀
    val join1_without_prefix: DataFrame = join1.map(row => {
      val key: String = row.getAs[String]("key")
      val value1: String = row.getAs[String]("value1")
      val value2: String = row.getAs[String]("value2")
      val without_prefix_key: String = key.split("_")(0)
      (without_prefix_key, value1, value2)
    }).toDF("key", "value1", "value2")

//    join1_without_prefix.show(10)

//    println("=============================join2")
    // df1_2 和 df2_2 相互join
    val join2: DataFrame = df1_2.join(df2_2, "key")
//    join2.show(10)

    // 将 join1_without_prefix 和 join2 连接起来
    val result: Dataset[Row] = join1_without_prefix.union(join2).distinct()


//    println(" ==================== result")
//    result.show(10)

    val result_size: Long = result.count()
    println(result_size)

    result_size
  }


}

object ResolveDataSkew {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder()
      .appName("item feature")
      .master("local[*]")
      .getOrCreate()

    // 构建倾斜数据
    val df1: DataFrame = spark.createDataFrame(Seq(
      ("A", "1"),
      ("A", "2"),
      ("A", "3"),
      ("B", "1"),
      ("C", "1")
    )).toDF("key", "value1")

    val df2: DataFrame = spark.createDataFrame(Seq(
      ("A", "100"),
      ("B", "90"),
      ("C", "60")
    )).toDF("key", "value2")

//    df1.union(df2).show()

    val dataSkew = new ResolveDataSkew(spark)

    for(x <- 1 to 100) {
      val size: Long = dataSkew.joinDataSkewToDeal(df1, df2, "A")
      if(size != 5){
        println("结果有错误")
      }
    }

  }
}
