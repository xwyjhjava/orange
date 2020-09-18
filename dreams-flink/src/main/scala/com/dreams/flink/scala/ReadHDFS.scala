package com.dreams.flink.scala

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @Package com.dreams.flink.scala
 * @author ming
 * @date 2020/9/18 16:40
 * @version V1.0
 * @description TODO
 */
object ReadHDFS {

  def main(args: Array[String]): Unit = {
    // 执行一次就结束
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val stream: DataStream[String] = env.readTextFile("hafs://node04/")
//    stream.print()
//    env.execute()

    // 持续监听文件
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val filePath = "./testfile/helloworld.txt"
    val format = new TextInputFormat(new Path(filePath))
    val stream: DataStream[String] = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10)
    stream.print()
    env.execute()


  }
}
