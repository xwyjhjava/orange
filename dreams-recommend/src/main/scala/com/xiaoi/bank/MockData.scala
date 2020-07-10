package com.xiaoi.bank

import java.time.{Duration, LocalDate, Period}
import java.util.UUID

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.Random
import java.util

import org.apache.spark.sql.types.StructType

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/6 18:00
 * @version V1.0
 * @description 创建模拟数据
 */
class MockData {

//  private val LOGGER = LoggerFactory.getILoggerFactory.getLogger(MockData.getClass.getName)


  private val random = new Random()

  // 主地址标识
  private val ADR_ARRAY = Array("H", "W", "O", "T", "1")

  // 卡人主地址城市代码（电话区号）
  private val CIT_CODE_ARRAY = Array(
    "0551","0771","0871","023","0371"
  )

  // 车辆状况
  private val VEH_FLG_ARRAY = Array("Y", "N")

  //产品号 todo 补充和北部湾产品类型符合的
  private val PROD_NO_ARRAY = Array("001", "002", "003")

  // 产品类型
  private val PROD_TYPE_ARRAY = Array("长期险", "短期险", "极短险")

  // 被保人和投保人关系
  private val PAYED_SAME_F_ARRAY = Array("两者相同", "两者不同")




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
   *
   * 生成时长、缴费数据
   * @return
   */
  def getTermAndFee(): Array[Float] ={

    // 每期保费 [10, 999)
    val permFee: Float = (random.nextInt(1000) + 10).toFloat
    // 缴费时长, 单位是年
    val meth: Int = random.nextInt(12) + 1
    // 本年度保费金额
    val yearFee: Float = permFee * 12
    // 总保费
    val totalFee: Float = yearFee * meth
    //保额
    val coverage: Float = (totalFee / 10000).formatted("%.2f").toFloat
    //保险时长数值
    val insuranceTerm : Int = meth + 10

    Array(coverage, insuranceTerm, permFee, meth, yearFee, totalFee)

  }


  /**
   * 生成缴别
   * @return
   */
  def getPAYTPERQ(): Int ={

    val payt: Int = random.nextInt(5) + 1
    payt

  }


  /**
   * 生成被保人和投保人关系
   * @return
   */
  def getPAYEDSAME(): String ={

    val index: Int = random.nextInt(2)
    PAYED_SAME_F_ARRAY(index)

  }

  /**
   * 生成被保人年龄
   * @return
   */
  def getINSUREAGE(): Int ={

    val age: Int = random.nextInt(61)
    age

  }


}


object MockData{

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)

  private val LOGGER = LoggerFactory.getILoggerFactory.getLogger(MockData.getClass.getName)




  def main(args: Array[String]): Unit = {

    // spark初始化
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("mock data")
      .master("local[*]")
      .getOrCreate()

    val sparkContext: SparkContext = sparkSession.sparkContext
    val sqlContext: SQLContext = sparkSession.sqlContext

    // 初始化
    val data = new MockData()

    //mock user data
//    mockUserData(sparkSession, data)
    //mock order data
//    mockOrderData(sparkSession, data)


    val orderDF: DataFrame = sparkSession.read.option("header", true)
      .option("inferSchema", true)
      .load("D:\\data\\cmb\\user")

    orderDF.coalesce(1)
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv("D:\\data\\cmb\\userCSV")

    println("======done======")




  }


  /**
   * 生成order数据
   * @param sparkSession
   * @param data
   */
  private def mockOrderData(sparkSession: SparkSession, data: MockData) = {
    //read user data
    val userDF: DataFrame = sparkSession.read
      .option("header", true)
      .option("inferSchema", true)
      .load("D:\\data\\cmb\\user")

    // 选择目标user
    val nbrArray: Array[String] = userDF.select("NBR").take(10000)
      .map(row => row.getAs[String]("NBR"))

    val orderList = new util.ArrayList[OrderSchema]()


    var i = 0
    for (i <- 0 to 9999) {

      val order = new OrderSchema()

      val nbr: String = nbrArray(i)
      val saleDate: String = data.getSALEDATE()
      val merchCode: String = data.getMERCHCODE()
      val merchName: String = data.getMERCHNAME()
      val prodNo: String = data.getPRODNO()
      val prodType: String = data.getTYPE()
      val array: Array[Float] = data.getTermAndFee()

      val coverage: Float = array(0)
      val insuranceDurationUnit: Int = 1
      val insuranceDurationValue: Int = array(1).toInt
      val paymentDurationUnit: Int = 1
      val paymentDurationValue: Int = array(3).toInt

      val paymentType: Int = data.getPAYTPERQ()
      val perFee: Float = array(2)
      val yearFee: Float = array(4)
      val totalFee: Float = array(5)

      val payedSame: String = data.getPAYEDSAME()
      val insureAge: Int = data.getINSUREAGE()


      order.setNBR(nbr)
      order.setSALE_DATE(saleDate)
      order.setMERCH_COD(merchCode)
      order.setMERCH_NAME(merchName)
      order.setPROD_NO(prodNo)
      order.setTYPE(prodType)
      order.setCOVERAGE(coverage)
      order.setINSURE_TERM_TY(insuranceDurationUnit)
      order.setINSURE_TERM(insuranceDurationValue)
      order.setMETHDD_TERM_TY(paymentDurationUnit)
      order.setMETHDD_TERM(paymentDurationValue)
      order.setPAYT_PERQ(paymentType)
      order.setPERM_FEE(perFee)
      order.setYEAR_FEE(yearFee)
      order.setTOT_FEE(totalFee)
      order.setPAYED_INSURED_SAME_F(payedSame)
      order.setINSURE_AGE(insureAge)


      orderList.add(order)

    }


    val orderDF: DataFrame = sparkSession.createDataFrame(orderList, classOf[OrderSchema])
    orderDF.show()

    orderDF.write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .parquet("D:\\data\\cmb\\order")

    println("===========done=========")
  }

  /**
   * 生成用户数据
 *
   * @param sparkSession
   * @param data
   */
  private def mockUserData(sparkSession: SparkSession, data: MockData) = {
    val list = new util.ArrayList[UserSchema]()

    var i = 0
    for (i <- 0 to 1000000) {

      val user = new UserSchema()

      //编号
      val nbr: String = data.getNBR()
      //年龄
      val age: Int = data.getAGE()
      //性别
      val sex: Int = data.getSEX()
      //国籍
      val nationality: Int = data.getCITYID()
      //民族
      val ethnic: Int = data.getETHGRP()
      //行业类别
      val industry: Int = data.getCORTYPE()
      //职业等级
      val jobClass: Int = data.getCCCODE()
      //职称
      val jobTitle: Int = data.getOCCODE()
      //学历
      val education: Int = data.getEDU()
      //主地址标识
      val address: String = data.getADRID()
      //卡人主地址城市代码
      val cityCode: String = data.getCITCODE()
      //收入
      val income: Float = data.getINC()
      //婚姻状况
      val marriage: Int = data.getMARSTS()
      //有无子女
      val child: Int = data.getCHILDFLG()
      //车辆状况
      val vehicle: String = data.getVEHFLG()
      //现住房屋状况
      val house: Int = data.getHOS()
      //进件身份
      val appCode: Int = data.getAPPCODE()


      user.setNBR(nbr)
      user.setSEX(sex)
      user.setAGE(age)
      user.setCITY_ID(nationality)
      user.setETH_GRP(ethnic)
      user.setCOR_TYPE(industry)
      user.setCC_COD(jobClass)
      user.setOC_COD(jobTitle)
      user.setEDU(education)
      user.setADR_ID(address)
      user.setCIT_COD(cityCode)
      user.setINC(income)
      user.setMAR_STS(marriage)
      user.setCHILD_FLAG(child)
      user.setVEH_FLG(vehicle)
      user.setHOS_STS(house)
      user.setAPP_INC_COD(appCode)

      list.add(user)

    }

    val userDF: DataFrame = sparkSession.createDataFrame(list, classOf[UserSchema])
    userDF.show()

    userDF.write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .parquet("D:\\data\\cmb\\user")

    println("save success")
  }
}
