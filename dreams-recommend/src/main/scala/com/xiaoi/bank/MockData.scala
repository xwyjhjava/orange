package com.xiaoi.bank

import java.util.UUID

import scala.util.Random

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/6 18:00
 * @version V1.0
 * @description 创建模拟数据
 */
class MockData {


  private val random = new Random()


  //mock user data
  /**
   *
   * @param num 生产的数据条数
   */
  def mockUser(num: Long): Unit ={


    // 用户号
    getNBR()


  }


  /**
   * 生成客户号
   */
  def getNBR(): String ={

    val uuid: String = UUID.randomUUID().toString.replace("-", "")
    uuid

  }


  /**
   * 生成性别
   * @return
   */
  def getSEX(): Int ={

    val sex: Int = random.nextInt(2) + 1
    sex

  }


  /**
   * 生成年龄
   */
  def getAGE(): Int ={

    val age: Int = random.nextInt(60 - 18 + 1) + 18
    age

  }


  /**
   * 生成国籍别
   * @return
   */
  def getCITYID(): Int ={

    val cityID: Int = random.nextInt(4 - 1 + 1) + 1
    cityID

  }

  /**
   * 生成民族
   * @return
   */
  def getETHGRP(): Int ={

    val eth: Int = random.nextInt(98)
    eth

  }




}


object MockData{

  def main(args: Array[String]): Unit = {

    val data = new MockData()

    println(data.getNBR())


    var a = 0;
    for (a <- 1 to 20){
      println(data.getCITYID())
    }


  }
}
