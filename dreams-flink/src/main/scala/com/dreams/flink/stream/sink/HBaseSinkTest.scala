package com.dreams.flink.stream.sink

import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.hadoop.conf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * @Package com.dreams.flink.stream.sink
 * @author ming
 * @date 2020/9/25 14:03
 * @version V1.0
 * @description 数据存储到HBase中
 */
object HBaseSinkTest {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //配置kafka
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    // 添加数据源
    val stream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), prop))

    stream.map(data => {
      val splits: Array[String] = data.split("\t")
      val monitorId: String = splits(0)
      (monitorId, 1)
    }).keyBy(_._1)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2 + value2._2)
        }
      })
      .process(new ProcessFunction[(String, Int), (String, Int)] {
        var htab: HTable = _
        override def open(parameters: Configuration): Unit = {
          // 配置HBase
          val conf: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "node02:2181")
          val tableName = "car_flow"
          htab = new HTable(conf, tableName)
        }

        override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          val timestamp: Long = ctx.timerService().currentProcessingTime()
          val put = new Put(Bytes.toBytes(value._1))
          put.addColumn(Bytes.toBytes("count"), Bytes.toBytes(timestamp), Bytes.toBytes(value._2))
          htab.put(put)
        }

        override def close(): Unit = {
          htab.close()
        }
      })
    environment.execute()
  }



}
