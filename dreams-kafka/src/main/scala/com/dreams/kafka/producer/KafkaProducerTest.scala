package com.dreams.kafka.producer

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}


/**
 * @Package com.dreams.kafka.producer
 * @author ming
 * @date 2020/1/7 13:12
 * @version V1.0
 * @description kafka的生产者
 */
object KafkaProducerTest {

  def main(args: Array[String]): Unit = {

    val pros = new Properties()
    pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.199.130:9092")
    pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](pros)
    while (true) {
      for (i <- 1 to 3; j <- 1 to 3) {
        val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("test", s"item$j", s"action$i")
        val records: Future[RecordMetadata] = producer.send(record)
        val metadata: RecordMetadata = records.get()
        val partition: Int = metadata.partition()
        val offset: Long = metadata.offset()
        println(s"item$j action$i, partition:$partition, offset:$offset")
      }
      Thread.sleep(5000)
    }

    producer.close()
  }
}
