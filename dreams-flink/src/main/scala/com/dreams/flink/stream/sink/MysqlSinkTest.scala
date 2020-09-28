package com.dreams.flink.stream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * @Package com.dreams.flink.stream.sink
 * @author ming
 * @date 2020/9/25 13:18
 * @version V1.0
 * @description TODO
 */
object MysqlSinkTest {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[String] = environment.socketTextStream("node02", 8888)

    // 定义连接和语句
    var conn: Connection = null
    var updatePst: PreparedStatement = null
    var insertPst: PreparedStatement = null
    val datasourceUrl = "jdbc:mysql://localhost:3306/world?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true"

    stream.flatMap(line => {
      val rest = new ListBuffer[(String, Int)]
      line.split(" ").foreach(word => rest += ((word, 1)))
      rest
    }).keyBy(_._1)
      .reduce((v1, v2) => {
        (v1._1, v1._2 + v2._2)
      })
      .addSink(new RichSinkFunction[(String, Int)] {
        // 打开数据库连接
        override def open(parameters: Configuration): Unit = {
//          super.open(parameters)
          conn = DriverManager.getConnection(datasourceUrl, "root", "xiaoi")
          updatePst = conn.prepareStatement("update wc set count = ? where world = ?")
          insertPst = conn.prepareStatement("insert into wc values(?, ?)")
        }

        // 业务逻辑处理
        override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
          updatePst.setInt(1, value._2)
          updatePst.setString(2, value._1)
          updatePst.execute()
          if(updatePst.getUpdateCount <= 0){
            insertPst.setString(1, value._1)
            insertPst.setInt(2, value._2)
            insertPst.execute()
          }
        }

        // 关闭数据库连接
        override def close(): Unit = {
//          super.close()
          updatePst.close()
          insertPst.close()
          conn.close()
        }
      })
    environment.execute()
  }

}
