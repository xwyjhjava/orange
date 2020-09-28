package com.dreams.flink.stream.sink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.mutable.ListBuffer

/**
 * @Package com.dreams.flink.stream.sink
 * @author ming
 * @date 2020/9/25 10:44
 * @version V1.0
 * @description  将world count 写入 redis
 */
object RedisSinkTest {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    // jedis pool config
    val jedisPoolConfig: FlinkJedisPoolConfig = new FlinkJedisPoolConfig
      .Builder()
      .setHost("node02")
      .setPort(6379)
      .setDatabase(3)
      .build()

    stream.flatMap(line => {
      val rest = new ListBuffer[(String, Int)]
      line.split(" ").foreach(word => rest += ((word, 1)))
      rest
    }).keyBy(_._1)
      .reduce((v1, v2) => {
        (v1._1, v1._2 + v2._2)
      })
      .addSink(new RedisSink(jedisPoolConfig, new RedisMapper[(String, Int)] {
        // 指定操作redis的命令
        override def getCommandDescription: RedisCommandDescription = {
          new RedisCommandDescription(RedisCommand.HSET, "wc")
        }
        override def getKeyFromData(data: (String, Int)): String = {
          data._1
        }
        override def getValueFromData(data: (String, Int)): String = {
          data._2 + ""
        }
      }))

    environment.execute()
  }

}
