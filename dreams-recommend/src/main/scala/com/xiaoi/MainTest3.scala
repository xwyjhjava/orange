package com.xiaoi

/**
 * @Package com.xiaoi
 * @author ming
 * @date 2020/3/26 10:44
 * @version V1.0
 * @description 测试用例
 */
class MainTest3 {

}



class Worker private {
  def getConnection() = {
    println("init connection")
  }
}

object  Worker{
  val worker = new Worker
  def getWorkInstance(): Worker ={
    worker.getConnection()
    worker
  }
}

object Job{
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 5){
      Worker.getWorkInstance()
    }
  }
}
