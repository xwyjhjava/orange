package com.dreams.flink.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/23 15:15
 * @version V1.0
 * @description 将两个数据流进行合并 (合并条件： 数据流中元素必须一致)
 *
 */
object UnionOperator {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1: DataStream[(String, Int)] = environment.fromCollection(List(("a", 1), ("b", 2)))
    val stream2: DataStream[(String, Int)] = environment.fromCollection(List(("c", 3), ("d", 4)))

    /** union */
    stream1.union(stream2).print()

    environment.execute()
  }


  

}
