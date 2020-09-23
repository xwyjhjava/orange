package com.dreams.flink.stream.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/23 17:35
 * @version V1.0
 * @description TODO
 */
object CoFlatMap {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[String] = environment.socketTextStream("node02", 8888)
    val stream2: DataStream[String] = environment.socketTextStream("node02", 9999)

    val intStream: DataStream[Int] = stream1.map(_.toInt)
    val connectStream: ConnectedStreams[Int, String] = intStream.connect(stream2)

    /**
     * 这种方式实现的CoFlatMap 要求Collector[R]的相同的，即要求返回类型是一致的
     */
    connectStream.flatMap(
      (x: Int, c: Collector[String]) => {
        c.collect(x + "")
      },
      (y: String, c: Collector[String]) => {
        y.split(" ").foreach(d => {
          c.collect(d)
        })
      }
    ).print()



  }

}
