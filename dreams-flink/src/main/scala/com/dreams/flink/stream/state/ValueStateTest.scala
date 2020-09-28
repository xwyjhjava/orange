package com.dreams.flink.stream.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
/**
 * @Package com.dreams.flink.stream.state
 * @author ming
 * @date 2020/9/25 16:19
 * @version V1.0
 * @description 需求： 检查车辆通过卡口的车辆是否发生的急加速
 */
object ValueStateTest {

  case class CarInfo(carId: String, speed: Long)

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    stream.map(data => {
      val splits: Array[String] = data.split(" ")
      CarInfo(splits(0), splits(1).toLong)
    }).keyBy(_.carId)
      .map(new RichMapFunction[CarInfo, String] {
        var lastTempSpeed: ValueState[Long] = _
        override def open(parameters: Configuration): Unit = {
          // 创建valuestate
          val descSpeedState = new ValueStateDescriptor[Long]("speed", createTypeInformation[Long])
          lastTempSpeed = getRuntimeContext.getState[Long](descSpeedState)
        }
        override def map(value: CarInfo): String = {
          // 从valueState中获取值
          val lastSpeed: Long = lastTempSpeed.value()
          // 对值进行更新
          lastTempSpeed.update(value.speed)
          // 业务逻辑判断
          if(lastSpeed != 0 && (value.speed - lastSpeed > 30)){
            "over speed: " + value.toString
          }else{
            value.carId
          }
        }
      }).print()

    environment.execute()
  }
}
