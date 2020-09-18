package com.dreams.flink.scala

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @Package com.dreams.flink
 * @author ming
 * @date 2020/8/25 17:37
 * @version V1.0
 * @description TODO
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并行度
    env.setParallelism(1)
    //数据流
    // socket、flatmap、map组合成一个chain
    val initStream: DataStream[String] = env.socketTextStream("192.168.199.133", 8888) // socket 默认并行度是 1
    val wordStream: DataStream[String] = initStream.flatMap(_.split(" ")).setParallelism(2).startNewChain()
    val pairStream: DataStream[(String, Int)] = wordStream.map((_, 1)).setParallelism(3)
    // keyby、sum、print组合成一个chain
    val keyByStream: KeyedStream[(String, Int), Tuple] = pairStream.keyBy(0)
    val restStream: DataStream[(String, Int)] = keyByStream.sum(1)
    restStream.print()
    // 启动任务
    env.execute("first word count job")
  }

}
