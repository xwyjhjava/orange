package com.dreams.kafka.consume

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @Package com.dreams.kafka.consume
 * @author ming
 * @date 2020/1/7 13:11
 * @version V1.0
 * @description kafka的消费者
 */
object KafkaConsumeTest {

  def main(args: Array[String]): Unit = {
    val pros = new Properties()
    pros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.199.130:9092")
    pros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    pros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])

    pros.put(ConsumerConfig.GROUP_ID_CONFIG, "bula")
    pros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
    //    pros.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true)

//    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](pros)

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](pros)

    consumer.subscribe(Pattern.compile("test"), new ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        println("onPartitionsRevoked")
        val iter: util.Iterator[TopicPartition] = partitions.iterator()
        while(iter.hasNext){
          println(iter.next())
        }
      }

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        println("onPartitionsAssigned")
        val iter: util.Iterator[TopicPartition] = partitions.iterator()
        while(iter.hasNext){
          println(iter.next())
        }

        //调用数据库seek数据的offset
        Thread.sleep(1000)
      }
    })

    val offMap = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    var record: ConsumerRecord[String, String] = null

    while(true){

      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(0))
      if(!records.isEmpty){
        println(s"--------------${records.count()}------------------")

        val iter: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        while(iter.hasNext){
          record = iter.next()
          val topic: String = record.topic()
          val partition: Int = record.partition()
          val offset: Long = record.offset()
          val key: String = record.key()
          val value: String = record.value()
          println(s"key: $key  value: $value  partition: $partition  offset: $offset ")
        }
      }
    }
  }
}
