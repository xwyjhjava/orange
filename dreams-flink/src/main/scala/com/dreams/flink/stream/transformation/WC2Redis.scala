package com.dreams.flink.stream.transformation

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/24 15:41
 * @version V1.0
 * @description word count 结果写到redis
 */
object WC2Redis {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    val restStream: DataStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    var jedis: Jedis = null

    restStream.map(new RichMapFunction[(String, Int), String] {

      // subtask 启动的时候，首先调用的方法
      override def open(parameters: Configuration): Unit = {
//        super.open(parameters)
        // 获取上下文
        val taskName: String = getRuntimeContext.getTaskName
        val subtaskName: String = getRuntimeContext.getTaskNameWithSubtasks
        jedis = new Jedis("node02", 6379)
        jedis.select(3)
      }
      // map 处理每一个元素
      override def map(value: (String, Int)): String = {
        jedis.set(value._1, value._2 + "")
        value._1
      }
      // subtask执行完毕前的时候，调用的方法(cancel)
      override def close(): Unit = {
//        super.close()
        jedis.close()
      }

    })

    environment.execute()
  }

}
