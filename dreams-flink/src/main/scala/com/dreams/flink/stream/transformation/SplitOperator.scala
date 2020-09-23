package com.dreams.flink.stream.transformation

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}

/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/23 15:17
 * @version V1.0
 * @description 根据某一个条件来拆分数据
 */
object SplitOperator {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[Long] = environment.generateSequence(1, 10)

    /** 过滤出奇偶, 偶数分到一个流(first), 奇数分到另外一个流(second) */
    // TODO: split方法被deprecated, use side output instead
    val splitStream: SplitStream[Long] = stream.split(x => {
      x % 2 match {
        case 0 => List("first")
        case 1 => List("second")
      }
    })
    // 通过标签 获取指定流
    splitStream.select("first").print()
    environment.execute()
  }
}
