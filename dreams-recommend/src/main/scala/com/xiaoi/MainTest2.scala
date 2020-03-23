package com.xiaoi

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
        
      })
      .foreach(println)




  }

}
