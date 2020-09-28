package com.dreams.flink.stream.state

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @Package com.dreams.flink.stream.state
 * @author ming
 * @date 2020/9/25 17:52
 * @version V1.0
 * @description 需求： 统计每辆车的车速总和
 */
object ReducingStateTest {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    stream.map(data => {
      val splits: Array[String] = data.split(" ")
      (splits(0), splits(1).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String, Long), (String, Long)] {
        // 定义reducing state
        var lastreduceSate: ReducingState[Long] = _

        override def open(parameters: Configuration): Unit = {
          // 创建reducing state
          val desc = new ReducingStateDescriptor[Long]("reducingState", new ReduceFunction[Long] {
            override def reduce(value1: Long, value2: Long): Long = {
              value1 + value2
            }
          }, createTypeInformation[Long])
          lastreduceSate = getRuntimeContext.getReducingState(desc)
        }

        override def map(value: (String, Long)): (String, Long) = {
          // 更新state
          lastreduceSate.add(value._2)
          // 获取sate的值
          val res: Long = lastreduceSate.get()
          (value._1, res)
        }
      }).print()

    environment.execute()
  }


}
