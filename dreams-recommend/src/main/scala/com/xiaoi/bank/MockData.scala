package com.xiaoi.bank

import java.time.{Duration, LocalDate, Period}
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

  // 主地址标识
  private val ADR_ARRAY = Array("H", "W", "O", "T", "1")

  // 卡人主地址城市代码（电话区号）
  private val CIT_CODE_ARRAY = Array(
    "0551","0771","0871","023","0371"
  )

  // 车辆状况
  private val VEH_FLG_ARRAY = Array("Y", "N")

  //产品号
  private val PROD_NO_ARRAY = Array("001", "002", "003")

  // 产品类型
  private val PROD_TYPE_ARRAY = Array("长期险", "短期险", "极短险")








  //mock user data
  /**
   *
   * @param num 生产的数据条数
   */
  def mockUser(num: Long): Unit ={


    // 用户号
    getNBR()


  }

  // ====================用户表数据生成=======================

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

  /**
   * 生成行业类别
   * @return
   */
  def getCORTYPE(): Int ={

    val corType: Int = random.nextInt(6 -1 + 1) + 1
    corType

  }


  /**
   * 生成职业等级
   * @return
   */
  def getCCCODE(): Int = {
    val ccCode: Int = random.nextInt(99 - 7 + 1) + 7
    ccCode

  }


  /**
   * 生成职称
   * @return
   */
  def getOCCODE(): Int ={

    val ocCode: Int = random.nextInt(99 - 1 + 1) + 1
    ocCode
  }


  /**
   * 生成学历
   * @return
   */
  def getEDU(): Int ={
    val edu: Int = random.nextInt(7)
    edu
  }


  /**
   * 生成主地址标识
   * @return
   */
  def getADRID(): String ={

    //随机生成[0, 4]的下标
    val index: Int = random.nextInt(5)
    ADR_ARRAY(index)

  }


  /**
   * 生成卡人主地址城市代码
   * @return
   */
  def getCITCODE(): String ={

    //随机生成[0, 4]的下标
    val index: Int = random.nextInt(5)
    CIT_CODE_ARRAY(index)

  }


  /**
   * 生成收入（年收）
   * @return
   */
  def getINC(): Float ={
    // 收入区间 [0 - 999000]
    val INC: Float = random.nextInt(100) * 1000 * 12
    INC

  }


  /**
   * 生成婚姻状况
   * @return
   */
  def getMARSTS(): Int ={

    val marSTS: Int = random.nextInt(4)
    marSTS

  }

  /**
   * 生成有无子女
   * @return
   */
  def getCHILDFLG(): Int ={

    val childFLG: Int = random.nextInt(2)
    childFLG
  }

  /**
   * 生成车辆状况
   * @return
   */
  def getVEHFLG(): String ={

    val index: Int = random.nextInt(2)
    VEH_FLG_ARRAY(index)

  }


  /**
   * 生成房屋状况
   */
  def getHOS(): Int ={

    val hosSTS: Int = random.nextInt(6)
    hosSTS

  }


  /**
   * 进件身份
   */
  def getAPPCODE(): Int ={

    val appCode: Int = random.nextInt(7)
    appCode

  }


  // ============订单表数据生成=========================

  //客户号的生成复用上面用户的


  /**
   * 生成成交时间
   * @return
   */
  def getSALEDATE(): String ={

    val now: LocalDate = LocalDate.now()
    // 随机一个数， 作为与当前日期间隔天数， [0, 180]
    val day: Int = random.nextInt(181)

    val date: LocalDate = now.minusDays(day)
    date.toString

  }


  /**
   * 生成保险公司代码
   * @return
   */
  def getMERCHCODE(): String ={
    "1001"
  }


  /**
   * 生成保险公司名称
   * @return
   */
  def getMERCHNAME(): String ={
    "太平人寿"
  }


  /**
   * 生成产品号
   * @return
   */
  def getPRODNO(): String ={

    val index: Int = random.nextInt(3)
    PROD_NO_ARRAY(index)

  }


  /**
   * 生成产品类型
   * @return
   */
  def getTYPE(): String ={

    val index: Int = random.nextInt(3)
    PROD_TYPE_ARRAY(index)

  }


  /**
   * 生成保额
   * @return
   */
  def getCOVERAGE(): Float ={

    1.0.toFloat

  }


  /**
   * 生成保险时长单位
   * @return
   */
  def getTERMTY(): Int ={

    val termTY: Int = random.nextInt(3) + 1
    termTY

  }

  /**
   * todo
   * 生成保险时长数值
   * @return
   */
  def getTERM(): Int ={

    val term: Int = random.nextInt(13)
    term

  }

  /**
   * 生成缴费时长单位
   * @return
   */
  def getMETHOD(): Int ={

    val method: Int = random.nextInt(3) + 1
    method

  }


  /**
   * todo
   * 生成缴费时长数值
   * @return
   */
  def getMETHODTERM(): Int ={

    val term: Int = random.nextInt(13)
    term

  }


  /**
   * 生成缴别
   * @return
   */
  def getPAYTPERQ(): Int ={

    val payt: Int = random.nextInt(5) + 1
    payt

  }

  // TODO: 每期保费



















}


object MockData{

  def main(args: Array[String]): Unit = {

    val data = new MockData()

    println(data.getNBR())


    var a = 0;
    for (a <- 1 to 20){
      println(data.getSALEDATE())
    }



  }
}
