package com.dreams.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @Package com.dreams.spark.knowledge
 * @author ming
 * @date 2020/11/24 16:35
 * @version V1.0
 * @description TODO
 */
object LearnBaseRDD {

  // 面向数据集操作
  /*
  * 带函数的非聚合： map, flatMap
  * 1. 单元素： union, cartesion 没有函数计算
  * 2. kv元素： cogroup, join    没有函数计算
  * 3. 排序
  * 4. 聚合计算： reduceBykey, combinerBykey
  *
   */



  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("api learn")
      .master("local")
      .getOrCreate()

    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2: RDD[Int] = sc.parallelize(List(3, 4, 5, 6, 7))
//    println("rdd1 partitions size == " + rdd1.partitions.size)
//    println("rdd2 partitions size == " + rdd2.partitions.size)


    // ==================== union(没有产生shuffle) ========================
//    val unionRDD: RDD[Int] = rdd1.union(rdd2)
//    println(unionRDD.partitions.size)
//    unionRDD.foreach(println)


    //  ====================笛卡尔积(没有产生shuffle, 数据直接拷贝到一个节点上) =======================
//    val cartesianRDD: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
//    cartesianRDD.foreach(println)

    // ====================交集(产生shuffle)===========================
//    val intersectionRDD: RDD[Int] = rdd1.intersection(rdd2)
//    intersectionRDD.foreach(println)


    // ==================== 差集(产生shuffle):  有方向的=========================
//    val subtractRDD: RDD[Int] = rdd1.subtract(rdd2)
//    subtractRDD.foreach(println)


    val kv1: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 11),
      ("zhangsan", 12),
      ("lisi", 13),
      ("wangwu", 14)
    ))

    val kv2: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 21),
      ("zhangsan", 22),
      ("lisi", 23),
      ("zhaoliu", 28)
    ))

    // ================cogroup=======================
//    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = kv1.cogroup(kv2)
    /* (zhangsan,(CompactBuffer(11, 12),CompactBuffer(21, 22)))
    (wangwu,(CompactBuffer(14),CompactBuffer()))
    (zhaoliu,(CompactBuffer(),CompactBuffer(28)))
    (lisi,(CompactBuffer(13),CompactBuffer(23))) */
//    cogroupRDD.foreach(println)


    // =============join, leftOuterJoin, rightOuterJoin, fullOuterJoin
//    val joinRDD: RDD[(String, (Int, Int))] = kv1.join(kv2)
    /*
    (zhangsan,(11,21))
    (zhangsan,(11,22))
    (zhangsan,(12,21))
    (zhangsan,(12,22))
    (lisi,(13,23))
     */
//    joinRDD.foreach(println)



//    val leftOuterJoinRDD: RDD[(String, (Int, Option[Int]))] = kv1.leftOuterJoin(kv2)
    /*
    (zhangsan,(11,Some(21)))
    (zhangsan,(11,Some(22)))
    (zhangsan,(12,Some(21)))
    (zhangsan,(12,Some(22)))
    (wangwu,(14,None))
    (lisi,(13,Some(23)))
     */
//    leftOuterJoinRDD.foreach(println)

//    val rightOuterJoinRDD: RDD[(String, (Option[Int], Int))] = kv1.rightOuterJoin(kv2)
    /*
    (zhangsan,(Some(11),21))
    (zhangsan,(Some(11),22))
    (zhangsan,(Some(12),21))
    (zhangsan,(Some(12),22))
    (zhaoliu,(None,28))
    (lisi,(Some(13),23))
     */
//    rightOuterJoinRDD.foreach(println)
//
//    val fullOuterJoinRDD: RDD[(String, (Option[Int], Option[Int]))] = kv1.fullOuterJoin(kv2)
    /*
    (zhangsan,(Some(11),Some(21)))
    (zhangsan,(Some(11),Some(22)))
    (zhangsan,(Some(12),Some(21)))
    (zhangsan,(Some(12),Some(22)))
    (wangwu,(Some(14),None))
    (zhaoliu,(None,Some(28)))
    (lisi,(Some(13),Some(23)))
     */
//    fullOuterJoinRDD.foreach(println)






    // ===================== 排序 ========================

    // 需求： 根据数据计算各网站的pv、uv同时只显示top5
    // 解法： pv、uv排序取top5

//    val fileRDD: RDD[String] = sc.textFile("data/pvuvdata", 5)

    //pv
    //43.169.217.152	河北	2018-11-12	1542011088714	3292380437528494072	www.dangdang.com	Login

//    println("=================== pv ===========================")
//    val parRDD: RDD[(String, Int)] = fileRDD.map(line => {
//      (line.split("\t")(5), 1)
//    })
//    val reduceRDD: RDD[(String, Int)] = parRDD.reduceByKey(_ + _)
//    val mapRDD: RDD[(Int, String)] = reduceRDD.map(_.swap)
//    val sortedRDD: RDD[(Int, String)] = mapRDD.sortByKey(false)
//    val resRDD: RDD[(String, Int)] = sortedRDD.map(_.swap)
//    val pv: Array[(String, Int)] = resRDD.take(5)
//    pv.foreach(println)


//    println(" ===================  uv =======================")
//    43.169.217.152	河北	2018-11-12	1542011088714	3292380437528494072	www.dangdang.com	Login
//    val keysRDD: RDD[(String, String)] = fileRDD.map(line => {
//    val arr: Array[String] = line.split("\t")
//      (arr(5), arr(0))
//    })
//
//    val keyRDD: RDD[(String, String)] = keysRDD.distinct()
//    val uvpairRDD: RDD[(String, Int)] = keyRDD.map(k => (k._1, 1))
//    val uvreduceRDD: RDD[(String, Int)] = uvpairRDD.reduceByKey(_ + _)
//    val uvSortedRDD: RDD[(String, Int)] = uvreduceRDD.sortBy(_._2, false)
//    val uv: Array[(String, Int)] = uvSortedRDD.take(5)
//    uv.foreach(println)


    // =============== 聚合 ===============================
    val dataRDD: RDD[(String, Int)] = sc.parallelize(List(
      ("zhangsan", 234),
      ("zhangsan", 5667),
      ("zhangsan", 343),
      ("lisi", 212),
      ("lisi", 44),
      ("lisi", 33),
      ("wangwu", 555),
      ("wangwu", 22)
    ))

//    val groupRDD: RDD[(String, Iterable[Int])] = dataRDD.groupByKey()
//    groupRDD.foreach(println)

//    println(" ====================================================== ")
    // 行列转换
//    val res01RDD: RDD[(String, Int)] = groupRDD.flatMap(e => e._2.map(x => (e._1, x)))
//    res01RDD.foreach(println)

//    println("====================================================")
//    groupRDD.flatMapValues(e => e.iterator).foreach(println)


//    println(" ================================================= ")
//    groupRDD.mapValues(e => e.toList.sorted.take(2)).foreach(println)
//    println(" ================================================ ")
//    groupRDD.flatMapValues(e => e.toList.sorted.take(2)).foreach(println)

//    println("====================sum,count,min,max,avg============================")
//    val sum: RDD[(String, Int)] = dataRDD.reduceByKey(_ + _)
//    val max: RDD[(String, Int)] = dataRDD.reduceByKey((ov, nv) => {
//      if (ov > nv) ov else nv
//    })
//    val min: RDD[(String, Int)] = dataRDD.reduceByKey((ov, nv) => {
//      if (ov < nv) ov else nv
//    })
//    val count: RDD[(String, Int)] = dataRDD.mapValues(e => 1).reduceByKey(_ + _)
//    val avg: RDD[(String, Int)] = sum.join(count).mapValues(e => e._1 / e._2)
//
//    println("====== sum =======")
//    sum.foreach(println)
//
//    println("====== max =======")
//    max.foreach(println)
//
//    println("====== min =======")
//    min.foreach(println)
//
//    println("====== count =====")
//    count.foreach(println)
//
//    println("===== avg ========")
//    avg.foreach(println)


    // =========================combineByKey===========================
//    println("=========combineByKey==========")
//
//    val tmpRDD: RDD[(String, (Int, Int))] = dataRDD.combineByKey(
//      /*
//       createCombiner: V => C,
//       mergeValue: (C, V) => C,
//       mergeCombiners: (C, C) => C)
//       */
//      // 第一条记录的value怎么放入hashmap
//      (value: Int) => (value, 1),
//      // 如果有第二条记录，以及以后的记录，value是怎么放到hashmap中
//      (oldValue: (Int, Int), newValue: Int) => (oldValue._1 + newValue, oldValue._2 + 1),
//      // 合并溢写结果
//      (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2)
//    )
//    tmpRDD.mapValues(e => e._1 / e._2).foreach(println)



    // ================================面向分区==========================================

    val data: RDD[Int] = sc.parallelize(1 to 10, 2)

    // 外关联SQL查询
    val res01: RDD[String] = data.map(value => {

      println("---conn--mysql")
      println(s"---select $value---")
      println("---close--mysql---")

      value + "select"

    })

//    res01.foreach(println)


//    println("=============面向分区操作================")

//    data.mapPartitions()
    val res02: RDD[String] = data.mapPartitionsWithIndex(
      (pindex, piter) => {

        // 致命的， spark就是一个pipline， 迭代器嵌套模式， 数据不会在内存中积压
        val lb = new ListBuffer[String]

        println(s"---$pindex---conn--mysql")
        while (piter.hasNext) {
          val value: Int = piter.next()
          println(s"---$pindex---select $value---")
          lb.+=(value + "select")
        }
        println("---close--mysql---")

        lb.iterator
      }
    )


//    println("=============面向分区的迭代器嵌套================")

    // 如果是一对多的输出，相当于需要重写flatMap
    val res03: RDD[String] = data.mapPartitionsWithIndex(
      (pindex, piter) => {
        new Iterator[String] {

          println(s"----$pindex---conn--mysql---")

          override def hasNext: Boolean = {
            if (piter.hasNext == false) {
              println(s"----$pindex---close--mysql---")
              false
            } else {
              true
            }
          }

          override def next(): String = {
            val value: Int = piter.next()
            println(s"---$pindex--select $value----")
            value + "select"
          }
        }
      }
    )
//    res03.foreach(println)



    // ====================================================

//    val data2: RDD[Int] = sc.parallelize(1 to 10, 5)

//    println("===============抽样=========================")
//    data2.sample(
//      /*
//      withReplacement: Boolean,  //是否抽重复元素
//      fraction: Double,  // 抽取的比例
//      seed: Long  //相同的种子，抽到的是同一批数据
//       */
//      false,
//      0.1,
//      1
//    ).foreach(println)
//
//    println("=====================")
//    data2.sample(false, 0.1, 1).foreach(println)
//    println("=====================")
//    data2.sample(true, 0.2, 2).foreach(println)

//    println("===========调优===============")

//    println(s"data: ${data2.getNumPartitions}")
//    val dataPartitionRDD: RDD[(Int, Int)] = data2.mapPartitionsWithIndex((pindex, piter) => {
//      piter.map(e => (pindex, e))
//    })
//    dataPartitionRDD.foreach(println)

//    println("=================================")

//    val reparitionRDD: RDD[(Int, Int)] = dataPartitionRDD.repartition(4)
    // coalesce
    // 分区数想变多,就必须产生shuffle, 即shuffle=true, 否则的话，分区数就变不了
    // 分区数想变少,可以不产生shuffle
//    val reparitionRDD: RDD[(Int, Int)] = dataPartitionRDD.coalesce(3, false)
//    println(s"repatition data: ${reparitionRDD.getNumPartitions}")
//    reparitionRDD.mapPartitionsWithIndex((pindex, piter) => {
//      piter.map(e => (pindex, e))
//    }).foreach(println)



    // ===========================topN、 分组排序===================

    println("===========topN、分组排序===================")

    implicit val myOrder: Ordering[(Int, Int)] = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compareTo(x._2)
    }


    val data3: RDD[String] = sc.textFile("data/tqdata")
//    data3.foreach(println)

//    2019-6-1	39
    // 同年月份中, 温度最高的2天

    val baseRDD: RDD[(Int, Int, Int, Int)] = data3.filter(_.split("\t").length == 2).map(e => {
      val arr: Array[String] = e.split("\t")
      val date: String = arr(0)
      val temperature: String = arr(1)
      val dateArr: Array[String] = date.split("-")
      // year, month, day, temperature
      (dateArr(0).toInt, dateArr(1).toInt, dateArr(2).toInt, temperature.toInt)
    })

    // 解法一
    // 风险点： 1. groupByKey可能会造成某个key的value特别多时, 有OOM的风险
    //         2. 申请了额外了HashMap空间
//    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = baseRDD.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
//    val res: RDD[((Int, Int), List[(Int, Int)])] = grouped.mapValues(arr => {
//      val map = new mutable.HashMap[Int, Int]()
//      arr.foreach(x => {
//        if (map.get(x._1).getOrElse(0) < x._2) {
//          map.put(x._1, x._2)
//        }
//      })
//      map.toList.sorted(new Ordering[(Int, Int)] {
//        override def compare(x: (Int, Int), y: (Int, Int)): Int = {
//          y._2.compareTo(x._2)
//        }
//      })
//    })
//    res.foreach(println)



    // 解法2
    // 解决了去重
//    val reduced: RDD[((Int, Int, Int), Int)] = baseRDD.map(t4 => ((t4._1, t4._2, t4._3), t4._4))
//      .reduceByKey((x, y) => if (x > y) x else y)
//    val mapped: RDD[((Int, Int), (Int, Int))] = reduced.map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
//    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = mapped.groupByKey()
//    grouped.mapValues(arr => arr.toList.sorted.take(2)).foreach(println)


    // 解法3
    // 虽然第一步完成了数据的全排序, 但是在后续的shuffle步骤中，可能会打乱顺序
    // 打乱排序的原因： 排序时是按照年月温度， 而reduce时是按照年月日。 年月日不是年月温度的子集，所以会造成排序打乱

//    val sorted: RDD[(Int, Int, Int, Int)] = baseRDD.sortBy(t4 => (t4._1, t4._2, t4._4), false)
//    val reduced: RDD[((Int, Int, Int), Int)] = sorted.map(t4 => ((t4._1, t4._2, t4._3), t4._4)).reduceByKey((x, y) => if (x > y) x else y)
//    val mapped: RDD[((Int, Int), (Int, Int))] = reduced.map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
//    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = mapped.groupByKey()
//    grouped.foreach(println)


    // 解法4
    // 只有当sorted后续的shuffle的key是sorted的子集时，排序才不会被打乱
    val sorted: RDD[(Int, Int, Int, Int)] = baseRDD.sortBy(t4 => (t4._1, t4._2, t4._4), false)
    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = sorted.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
    grouped.mapValues(arr => arr.toList.take(2)).foreach(println)


    // 解法5

    val kv: RDD[((Int, Int), (Int, Int))] = baseRDD.map(t4 => ((t4._1, t4._2), (t4._3, t4._4)))
    kv.combineByKey(
      // 第一条记录的value怎么放
      (v:(Int, Int)) => {Array(v, (0,0),(0,0))},
      // 第二条以及后续的怎么做
      (oldValue:Array[(Int, Int)], newValue:Array[(Int, Int)]) => {

      }

    )




























    while (true){}

  }

}
