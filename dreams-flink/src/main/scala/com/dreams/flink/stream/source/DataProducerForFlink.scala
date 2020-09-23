package com.dreams.flink.stream.source

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.{BufferedSource, Source}

/**
 * @Package com.dreams.flink.stream
 * @author ming
 * @date 2020/9/20 23:34
 * @version V1.0
 * @description 生产数据到kafka， 为flink服务
 */
object DataProducerForFlink {

  def main(args: Array[String]): Unit = {
    // 初始化配置文件
    val prop = new Properties()

    // 设置 bootstrap.servers 和 key,value序列化
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092")
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](prop)
    val source: BufferedSource = Source.fromFile("./data/carFlow_all_column_test.txt")
    val iterator: Iterator[String] = source.getLines()
//    val size: Int = iterator.toList.size
//    println("size => " + size)
    val carList: List[String] = iterator.toList

    for(i <- 1 to 10){
      for( elem <- carList){
        val builder = new StringBuilder
        val array: Array[String] = elem.split(",")
        val monitorId: String = array(0).replace("'", "")
        val carId: String = array(2).replace("'", "")
        val timestamp: String = array(4).replace("'", "")
        val speed: String = array(6)
        val carInfo: StringBuilder = builder.append(monitorId + "\t").append(carId + "\t")
          .append(timestamp + "\t").append(speed + "\t")
        // 构造produceRecord
        println(i + "--" + carInfo.toString())
        val record = new ProducerRecord[String, String]("flink-kafka", i + "", carInfo.toString())
        // 发送数据
        producer.send(record)
        Thread.sleep(1000)
      }
    }
  }
}
