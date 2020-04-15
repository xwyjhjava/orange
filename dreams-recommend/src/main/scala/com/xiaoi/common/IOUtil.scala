package com.xiaoi.common

object IOUtil {
  /**
    * java接口写本地磁盘, isAppend: Default overwirte, set true to append
    * @param result
    */
  def writeToLocalFile(result: Array[String],
                       file_path: String,
                       isAppend: Boolean = false): Unit ={
    import java.io._
    try {
      val out = new FileWriter(file_path,isAppend)
      for (line <- result) {
        out.write(line)
        out.write("\n")
      }
      out.close()
    } catch {
      case ex: IOException => println("Write File Error")
    }
  }

}
