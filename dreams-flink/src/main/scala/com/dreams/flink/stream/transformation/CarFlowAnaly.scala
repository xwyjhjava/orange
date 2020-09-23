package com.dreams.flink.stream.transformation

import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @Package com.dreams.flink.stream.transformation
 * @author ming
 * @date 2020/9/23 16:14
 * @version V1.0
 * @description
 */
object CarFlowAnaly {

  def main(args: Array[String]): Unit = {
    //kafka 配置信息
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-kafka-001")

    //stream 上下文
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 添加Source
    val stream: DataStream[(String, String)] = environment.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka", new KafkaDeserializationSchema[(String, String)] {

      // 什么时候停止， 停止条件是什么
      override def isEndOfStream(t: (String, String)): Boolean = false

      // 要进行序列化的字节流
      override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
        val key = new String(consumerRecord.key(), "UTF-8")
        val value = new String(consumerRecord.value(), "UTF-8")
        (key, value)
      }

      // 指定一下返回的数据类型， Flink提供的类型
      override def getProducedType: TypeInformation[(String, String)] = {
        createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }, prop))



    /**
     * 相同key的数据 一定是由某一个subtask处理
     * 一个subtask可能会处理多个key所对应的数据
     */

    // 过滤掉key
    val valueStream: DataStream[String] = stream.map(_._2)

    //    需求1： 从kafka消费数据， 统计各个卡口的流量
    valueStream.map(data => {
      val arr: Array[String] = data.split("\t")
      val monitorId: String = arr(0)
      (monitorId, 1)
    }).keyBy(x => x._1)
        .reduce(new ReduceFunction[(String, Int)] {
          override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
            (value1._1, value1._2 + value2._2)
          }
        }).print()


    // 需求2： 从kafka消费数据， 统计每一分钟每个卡口的流量
    // 解决：  构建组合key
    valueStream.map(data => {
      val arr: Array[String] = data.split("\t")
      val monitorId: String = arr(0)
      val timestamp: String = arr(2).substring(0, 16)
      (timestamp + "_"+ monitorId, 1)
    }).keyBy(v => v._1)
        .reduce((v1, v2) => {
          (v1._1, v1._2 + v2._2)
        }).print()






    environment.execute()
  }

}
