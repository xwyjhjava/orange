package com.dreams.flink.stream.sink

import java.lang
import java.util.Properties

import akka.remote.serialization.StringSerializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer

/**
 * @Package com.dreams.flink.stream.sink
 * @author ming
 * @date 2020/9/25 11:27
 * @version V1.0
 * @description
 */
object KafkaSinkTest {

  def main(args: Array[String]): Unit = {
    // kafka配置信息
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092")
//    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    stream.flatMap(line => {
      val rest = new ListBuffer[(String, Int)]
      line.split(" ").foreach(word => rest += ((word, 1)))
      rest
    }).keyBy(_._1)
      .reduce((v1, v2) => {
        (v1._1, v1._2 + v2._2)
      })
      .addSink(new FlinkKafkaProducer[(String, Int)](
        // topic
        "wc",
        new KafkaSerializationSchema[(String, Int)] {
          override def serialize(t: (String, Int), aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
            new ProducerRecord[Array[Byte], Array[Byte]]("wc", t._1.getBytes, t._2.toString.getBytes())
          }
        },
        prop,
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE
      ))
    environment.execute()
  }


}
