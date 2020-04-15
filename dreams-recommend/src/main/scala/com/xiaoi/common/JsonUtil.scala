package com.xiaoi.common

import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

/**
  * JsonUtil
  *
  * Created by ligz on 16/3/8.
  */
object JsonUtil {

  implicit val format = DefaultFormats

  /**
    * 将得到的topK（w，cnt）转换为Json格式
    *
    * @param ws
    * @param dt
    * @return
    */
  def toJson(ws: List[(String, Int)], dt: String): String = {
    val json = ("time" -> dt) ~
      ("info" ->
        ws.map{ case (w, cnt) => ("word" -> w) ~ ("cnt" -> cnt) }
        )
    compact(render(json))
  }

  def toJson[T](data: StatInfo[T]): String = {
    write(data)
  }

  /**
    * 转换任意Scala的引用类型到json字符串
    *
    * @param obj
    * @return
    */
  def toJson(obj: AnyRef): String = {
    write(obj)
  }

  /**
    * 将json反序列化成对象
    *
    * @param json
    * @return
    */
  def toObj(json: String): JValue = {
    parse(json)
  }

  case class StatInfo[T](time: String, info: List[T])

  case class StatRecord(word: String, cnt: Int)

}
