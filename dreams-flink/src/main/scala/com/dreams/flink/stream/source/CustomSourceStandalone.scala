package com.dreams.flink.stream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * @Package com.dreams.flink.stream.source
 * @author ming
 * @date 2020/9/23 14:15
 * @version V1.0
 * @description 自定义数据源
 */
object CustomSourceStandalone {
  def main(args: Array[String]): Unit = {
    // 上下文
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 添加数据源
    // SourceFunction 接口只支持单并行度
    val stream: DataStream[String] = environment.addSource(new SourceFunction[String] {
      var flag = true
      // run 实际可以读取任何地方的数据， 然后将数据发送出去（向下一个环节传递）
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        val random = new Random()
        while (true) {
          ctx.collect("hello-" + random.nextInt(100))
          Thread.sleep(500)
        }
      }
      override def cancel(): Unit = {
        flag = false
      }
    })
    stream.print()
    environment.execute()
  }
}
