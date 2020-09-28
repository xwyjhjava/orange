package com.dreams.flink.stream.transformation

import org.apache.commons.math3.analysis.function.StepFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/24 14:54
 * @version V1.0
 * @description TODO
 */
object IteratorOperator {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)
    val longStream: DataStream[Long] = stream.map(_.toLong)

    longStream.iterate{
      iter => {
        // 定义迭代逻辑
        val iterationBody: DataStream[Long] = iter.map(x => {
          println("---" + x)
          if (x > 0) x - 1
          else x
        })
        // >0则 继续返回到stream流中， <=0 继续往下游发送
        (iterationBody.filter(_ > 0), iterationBody.filter(_ <= 0))
      }
    }.print()

    environment.execute()
  }

}
