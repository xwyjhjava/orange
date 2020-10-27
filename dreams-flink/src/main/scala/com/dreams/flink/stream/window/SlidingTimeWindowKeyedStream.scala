package com.dreams.flink.stream.window

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @Package com.dreams.flink.stream.window
 * @author ming
 * @date 2020/10/23 17:26
 * @version V1.0
 * @description 读取kafka数据， 统计每隔10s最近30分钟内， 每辆车的平均速度
 */
object SlidingTimeWindowKeyedStream {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = environment.socketTextStream("", 888)

    stream.map(data=> {
      val array: Array[String] = data.split("\t")
      // 车牌号， 车速
      (array(1) , array(3).toLong)
    })
      .keyBy(_._1)
      .timeWindow(Time.minutes(30), Time.seconds(10))
      //    （车牌号, 车速） , (车牌号, 速度总和, 经过了多少次卡口),  (车牌号, 平均速度)
      .aggregate(new AggregateFunction[(String, Long), (String, Long, Long), (String, Double)]{
        // 创建一个累加器
        override def createAccumulator(): (String, Long, Long) = ("", 0, 0)

        override def add(value: (String, Long), accumulator: (String, Long, Long)): (String, Long, Long) = {
          (value._1, value._2 + accumulator._2, accumulator._3 + 1)
        }

        override def getResult(accumulator: (String, Long, Long)): (String, Double) = {
          (accumulator._1, accumulator._2.toDouble / accumulator._2)
        }

        override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) = {
          (a._1, a._2 + b._2, a._3 + a._3)
        }
      })
      .print()

    environment.execute()
  }

}
