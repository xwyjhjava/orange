package com.dreams.flink.stream.state

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

/**
 * @Package com.dreams.flink.stream.state
 * @author ming
 * @date 2020/9/25 17:43
 * @version V1.0
 * @description TODO
 */
object AggregateStateTest {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    stream.map(data => {
      val splits: Array[String] = data.split(" ")
      (splits(0), splits(1).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String, Long), (String, Long)] {
        // 定义agg state
        var lastaggState: AggregatingState[Long, Long] = _

        override def open(parameters: Configuration): Unit = {
          // 创建aggregate state
          val desc = new AggregatingStateDescriptor[Long, Long, Long]("aggState", new AggregateFunction[Long, Long, Long] {

            //初始化一个累加器
            override def createAccumulator(): Long = 0

            //每条数据都会调用一次(即每条数据的处理逻辑)
            override def add(value: Long, accumulator: Long): Long = value + accumulator

            // 返回的结果
            override def getResult(accumulator: Long): Long = accumulator

            // 累加器的merge
            override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
          }, createTypeInformation[Long])
          lastaggState = getRuntimeContext.getAggregatingState(desc)
        }

        override def map(value: (String, Long)): (String, Long) = {
          // 更新state
          lastaggState.add(value._2)
          // 获取sate的值
          val res: Long = lastaggState.get()
          (value._1, res)
        }
      }).print()

    environment.execute()
  }
}
