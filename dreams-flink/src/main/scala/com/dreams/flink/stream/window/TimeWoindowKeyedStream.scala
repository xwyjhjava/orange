package com.dreams.flink.stream.window

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @Package com.dreams.flink.stream.window
 * @author ming
 * @date 2020/10/22 19:01
 * @version V1.0
 * @description TODO
 */
object TimeWoindowKeyedStream {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = environment.socketTextStream("192.168.199.132", 8888)

    val wordStream: DataStream[String] = stream.flatMap(_.split(" "))
    val pairStream: DataStream[(String, Int)] = wordStream.map((_, 1))
    //
    val keyByStream: KeyedStream[(String, Int), String] = pairStream.keyBy(_._1)






    environment.execute()


  }


}
