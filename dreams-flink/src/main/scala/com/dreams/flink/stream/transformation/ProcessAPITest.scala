package com.dreams.flink.stream.transformation

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/24 16:03
 * @version V1.0
 * @description 需求： 监控每辆汽车， 车速超过100迈， 5s钟后发出警告
 */
// 定时器适用场景： 数据延迟
object ProcessAPITest {

  case class CarInfo(carId: String, speed: Long)
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    stream.map(data => {
      val splits: Array[String] = data.split(" ")
      val carId: String = splits(0)
      val speed: Long = splits(1).toLong
      CarInfo(carId, speed)
    }).keyBy(_.carId)
      //KeyedStream调用process需要传入KeyedProcessFunction
      //DataStream调用process需要传入ProcessFunction
      .process(new KeyedProcessFunction[String, CarInfo, String] {

        override def processElement(value: CarInfo, ctx: KeyedProcessFunction[String, CarInfo, String]#Context, out: Collector[String]): Unit = {
          //得到当前处理时间
          val currentTime: Long = ctx.timerService().currentProcessingTime()
          // 车速大于100时， 设置一个定时器
          if(value.speed > 100){
            val timerTime: Long = currentTime + 5 * 1000
            //定时器注册到ctx中
            ctx.timerService().registerProcessingTimeTimer(timerTime)
          }
        }

        // 重写定时处理逻辑
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, CarInfo, String]#OnTimerContext, out: Collector[String]): Unit = {
          super.onTimer(timestamp, ctx, out)
          val warnMsg: String = "warn...time" + timestamp + " carId..." + ctx.getCurrentKey
          out.collect(warnMsg)
        }
      }).print()

    environment.execute()
  }

}
