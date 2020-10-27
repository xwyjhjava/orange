package com.dreams.flink.stream.window

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Package com.dreams.flink.stream.window
 * @author ming
 * @date 2020/10/22 19:01
 * @version V1.0
 * @description TODO
 */
object TimeWindowKeyedStream {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = environment.socketTextStream("192.168.199.132", 8888)

    val wordStream: DataStream[String] = stream.flatMap(_.split(" "))
    val pairStream: DataStream[(String, Int)] = wordStream.map((_, 1))
    // 一个已经分好流的无界流
    val keyByStream: KeyedStream[(String, Int), String] = pairStream.keyBy(_._1)
    keyByStream.timeWindow(Time.seconds(10))
        .reduce(new ReduceFunction[(String, Int)]{
          override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
            (value1._1, value1._2 + value2._2)
          }
          // 窗口结束会计算一次
        }, new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {

            // 窗口的开始和结束时间
            // 滚动窗口时间片不一定是连续的， 在没有数据的时候， flink 不会创建窗口去做计算 （基于事件的触发机制）
            val start: Long = window.getStart
            val end: Long = window.getEnd

            val connection: Connection = DriverManager.getConnection("jdbc:mysql://node01:3306/test", "root", "xiaoi")
            val statement: PreparedStatement = connection.prepareStatement("insert into wc_time values(?, ?, ?, ?)")

            val head: (String, Int) = input.head
            statement.setLong(1, start)
            statement.setLong(2, end)
            statement.setString(3, head._1)
            statement.setInt(4, head._2)

            statement.execute()
            statement.close()
            connection.close()

            // 向下游发送数据, 这样print才会打印出来
//            out.collect(head)
          }
        })
        .print()

    environment.execute()


  }


}
