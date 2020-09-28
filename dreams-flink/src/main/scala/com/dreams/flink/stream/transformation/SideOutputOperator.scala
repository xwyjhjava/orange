package com.dreams.flink.stream.transformation

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/24 14:19
 * @version V1.0
 * @description TODO
 */
object SideOutputOperator {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    val gtTag = new OutputTag[String]("gt")
    val processStream: DataStream[String] = stream.process(new ProcessFunction[String, String] {
      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        try {
          val longVar = value.toLong
          if (longVar > 100) {
            // 往主流里写
            out.collect(value)
          } else {
            // 往测流里写
            ctx.output(gtTag, value)
          }
        } catch {
          case e => e.printStackTrace()
            ctx.output(gtTag, value)
        }
      }
    })
    // 获取测流数据
    val sideStream: DataStream[String] = processStream.getSideOutput(gtTag)
    sideStream.print("sideStream")
    //默认情况下， print打印的是主流
    processStream.print("mainStream")
    environment.execute()
  }

}
