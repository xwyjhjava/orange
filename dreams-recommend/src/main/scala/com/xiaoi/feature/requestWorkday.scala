package com.xiaoi.feature

import java.io.{File, PrintWriter}

import scopt.OptionParser
import scalaj.http.{Http, HttpResponse}
import com.github.nscala_time.time.Imports._
import com.xiaoi.common.StrUtil

object requestWorkday {

  def run(params: Params): Unit = {

    def plusOneDay(time: String, days: Int): String = {
      var date = DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(time)
      val yesterday = (date + days.days).toString("yyyy-MM-dd")
      yesterday
    }
    val time = params.time
    val year = time.trim().substring(0, 4)
    val outputPath = params.outputPath
    val writer = new PrintWriter(new File(outputPath))
    println(s"request calendar_workday of ${year} and save in :${outputPath}")
    for (num <- 0 to 364) {
      var day = plusOneDay(time, num)
      //工作日对应结果为 0, 休息日对应结果为 1, 节假日对应的结果为 2
      val realUrl = "http://tool.bitefu.net/jiari/?d=" + day.replaceAll("-", "")
      val response = Http(realUrl).timeout(connTimeoutMs = 8000, readTimeoutMs = 8000).asString
      var body = response.body.toString
      while(!StrUtil.isDecimalNum(body)){
        println("invalid return value! request again.")
        body= Http(realUrl).timeout(connTimeoutMs = 8000, readTimeoutMs = 8000).asString.body.toString

      }
      writer.write(day + "," + body + "\n")
    }
    writer.close()
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("Question Filter Locally") {
      opt[String]("inputPath")
        .text("输入文件路径")
        .action((x, c) => c.copy(inputPath = x))
      checkConfig { params =>
        success
      }
    }
    parser.parse(args, defaultParams).map { params => run(params) }
      .getOrElse(sys.exit(1))

  }
  case class Params(
                     time: String = "2018-01-01",
                     inputPath: String = "/xiaoi/joshTest/recommend/zhijian/calendar",
                     outputPath: String = "xiaoi/ch-recommend/data/calendar"
                   )
}
