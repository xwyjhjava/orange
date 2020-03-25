package com.xiaoi

import org.json4s.jackson.Json

import scala.collection.mutable.ListBuffer

/**
 * @Package com.xiaoi
 * @author ming
 * @date 2020/3/23 17:59
 * @version V1.0
 * @description 测试类2
 */
object MainTest2 {

  def main(args: Array[String]): Unit = {

    val tmpList = List(
      ("a", 1),
      ("b", 1),
      ("c", 1),
      ("a", 1),
      ("a", 1),
      ("c", 1)
    )

    val total_count: Int = tmpList.size

    println(total_count)

    println("================")

    tmpList.groupBy(_._1)
      .mapValues(ele => {
        val size: Int = ele.size
        size
      })
      .map(e => {

        val itemId: String = e._1
        val value: Int = e._2

        // 和处理日期、季节的操作相似
        (itemId, value)
      })
      .foreach(println)


    val buffer = new ListBuffer[String]()
    buffer.append(1 + "品类排名靠前")
    buffer.append(2 + "品类排名居中")
    buffer.append(3 + "品类排名靠后")


    val buffer2 = new ListBuffer[String]()
    buffer2.append(11 + "品类排名靠前")
    buffer2.append(22 + "品类排名居中")
    buffer2.append(33 + "品类排名靠后")


    val tmpList2 = List(
      buffer.toList,
      buffer2.toList
    )

    tmpList2.foreach(println)

    println("==================")

    tmpList2.flatMap(ele => {
      ele
    })
      .foreach(println)



    println(" =================== ")

    val row_1: Integer = 12344
    val row_2: String  = "同品类排名靠前"

    val str: String = "{\"item_id\":" + row_1 + "," + "\"label\":" + row_2 + "}"
    val jsonString: String = Json.formatted(str)

    println(jsonString)



  }

}
