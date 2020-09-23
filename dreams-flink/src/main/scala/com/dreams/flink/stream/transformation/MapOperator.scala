package com.dreams.flink.stream.transformation

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/23 14:36
 * @version V1.0
 * @description TODO
 */
object MapOperator {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    /** generateSequence一般用作测试
    val localStream: DataStream[Long] = environment.generateSequence(1, 100) */


    /**
     *  使用flatMap代替filter
     *  数据中包含 abc， 则过滤
     *

    stream.flatMap(x => {
      val rest = new ListBuffer[String]
      if(!x.contains("abc")){
        rest += x
      }
      rest.iterator
    }).print()
     */


    /** keyBy 算子

    stream.flatMap(_.split(" "))
        .map((_, 1))
        .keyBy(new KeySelector[(String, Int), String]{
          override def getKey(value: (String, Int)): String = {
            value._1
          }
        })
        .sum(1)
        .print()*/


    /** reduce 算子
    stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(new KeySelector[(String, Int), String]{
        override def getKey(value: (String, Int)): String = {
          value._1
        }
      })
      .reduce((v1, v2) => {
        (v1._1, v1._2 + v2._2)
      })
      .print()*/



    /** 使用reduce实现去重  */
    stream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(new KeySelector[(String, Int), String]{
        override def getKey(value: (String, Int)): String = {
          value._1
        }
      })
      .reduce((v1, v2) => {
        (v1._1, v1._2 + v2._2)
      })
        .filter(v => v._2 == 1).print()
    environment.execute()


  }

}
