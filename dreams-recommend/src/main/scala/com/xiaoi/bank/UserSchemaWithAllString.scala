package com.xiaoi.bank

import scala.beans.BeanProperty

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/13 11:13
 * @version V1.0
 * @description TODO
 */
class UserSchemaWithAllString {

  //编号
  @BeanProperty
  var NBR : String = ""
  // 年龄
  @BeanProperty
  var SEX : String = ""
  // 性别
  @BeanProperty
  var AGE : String = ""
  // 国籍
  @BeanProperty
  var CITY_ID: String = ""
  // 民族
  @BeanProperty
  var ETH_GRP : String = ""
  // 行业类别
  @BeanProperty
  var COR_TYPE : String = ""
  // 职业等级
  @BeanProperty
  var CC_COD : String = ""
  // 职称
  @BeanProperty
  var OC_COD : String = ""
  // 学历
  @BeanProperty
  var EDU: String = ""
  // 主地址标识
  @BeanProperty
  var ADR_ID: String = ""
  // 卡人主地址城市代码
  @BeanProperty
  var CIT_COD: String = ""
  // 收入
  @BeanProperty
  var INC : String = ""
  // 婚姻状况
  @BeanProperty
  var MAR_STS : String = ""
  //有无子女
  @BeanProperty
  var CHILD_FLAG : String = ""
  //车辆状况
  @BeanProperty
  var VEH_FLG : String = ""
  // 现住房屋状况
  @BeanProperty
  var HOS_STS : String = ""
  // 进件身份
  @BeanProperty
  var APP_INC_COD : String = ""

}
