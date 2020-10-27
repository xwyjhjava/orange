package com.dreams.flink.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._


/**
 * @Package com.dreams.flink.stream.window
 * @author ming
 * @date 2020/10/23 17:58
 * @version V1.0
 * @description 当窗口中有10个元素， 统计这个10个元素的wordcount
 */
object TubmingCountKeyedStream {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("192.168.199.132", 8888)
    // 如果基于keyed stream之上做 count window, 看的是每一个key是否大于10
    stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .countWindow(10)
      .reduce(new ReduceFunction[(String, Int)]{
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2 + value2._2)
        }
      })

  }

}
