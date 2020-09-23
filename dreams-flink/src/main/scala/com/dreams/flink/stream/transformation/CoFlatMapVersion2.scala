package com.dreams.flink.stream.transformation

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/23 18:11
 * @version V1.0
 * @description
 */
object CoFlatMapVersion2 {

  /**
   * 需求3 : 有一个配置文件存储车牌号和车主真实姓名
   *         通过数据流中的车牌号实时匹配出对应的车主姓名
   *         (注意： 配置文件可能实时改变)
   */
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 这里的并行度必须设置为1
    // TODO: 这里没弄清楚为什么并行度大于1的时候会出错
    environment.setParallelism(1)

    val carStream: DataStream[String] = environment.socketTextStream("node02", 8888)

    val filePath = "data/carId2Name"
    val nameStream: DataStream[String] = environment.readFile(new TextInputFormat(new Path(filePath)),
      filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 2000)

    carStream.connect(nameStream).map(new CoMapFunction[String, String, String] {
      // 车主姓名加载到Map中， 每一个thread线程都持有这样一份map（后续可优化为广播流）
      private val nameMap = new mutable.HashMap[String, String]()
      // 处理car的数据流
      override def map1(value: String): String = {
        nameMap.getOrElse(value, "not found name")
      }
      // 处理车主姓名的文件流
      override def map2(value: String): String = {
        val splits: Array[String] = value.split(" ")
        nameMap.put(splits(0), splits(1))
        println("id --" + splits(0))
        println("name --" + splits(1))
        println("size --" + nameMap.size)
        value + " --- 加载完毕"

      }
    }).print()


    environment.execute()
  }

}
