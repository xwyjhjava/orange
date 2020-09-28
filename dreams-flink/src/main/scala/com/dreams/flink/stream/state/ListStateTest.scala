package com.dreams.flink.stream.state

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

/**
 * @Package com.dreams.flink.stream.state
 * @author ming
 * @date 2020/9/25 16:51
 * @version V1.0
 * @description 统计每一辆车的运行轨迹
 *             1、拿到每一辆车的所有信息(车牌号、卡口号、通过卡口时间戳、车速)
 *             2、根据每辆车分组
 *             3、对每组数据中的信息按照时间戳升序，卡口连接起来，轨迹
 */
object ListStateTest {

  case class CarInfo(monitorId: String, carId: String, eventtime: Long, speed: Long)

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //kafka 配置信息
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092")
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-kafka-001")

    val stream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), prop))

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    stream.map(data => {
      val splits: Array[String] = data.split("\t")
      val time: Long = format.parse(splits(2)).getTime
      CarInfo(splits(0), splits(1), time, splits(3).toLong)
    }).keyBy(_.carId)
      .map(new RichMapFunction[CarInfo, (String, String)] {
        // 定义list state
        var lastCarState: ListState[(String, Long)] = _
        override def open(parameters: Configuration): Unit = {
          val desc = new ListStateDescriptor[(String, Long)]("lastCarState", createTypeInformation[(String, Long)])
          lastCarState = getRuntimeContext.getListState(desc)
        }

        override def map(value: CarInfo): (String, String) = {
          //更新state
          lastCarState.add((value.carId, value.eventtime))

          // 获取state值
          val carList: List[(String, Long)] = lastCarState.get().asScala.seq.toList
          val sortedList: List[(String, Long)] = carList.sortBy(_._2)

          val builder = new StringBuilder
          sortedList.foreach(ele => {
            builder.append(ele._1 + "\t")
          })
          (value.carId, builder.toString())
        }
      }).print()

    environment.execute()

  }

}
