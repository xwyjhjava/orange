package com.xiaoi.bank

import scala.beans.BeanProperty

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/8 16:51
 * @version V1.0
 * @description TODO
 */
class UserSchema extends Serializable {

  //编号
  @BeanProperty
  var NBR : String = ""
  // 年龄
  @BeanProperty
  var SEX : Int = 0
  // 性别
  @BeanProperty
  var AGE : Int = 0
  // 国籍
  @BeanProperty
  var CITY_ID: Int = 0
  // 民族
  @BeanProperty
  var ETH_GRP : Int = 0
  // 行业类别
  @BeanProperty
  var COR_TYPE : Int = 0
  // 职业等级
  @BeanProperty
  var CC_COD : Int = 0
  // 职称
  @BeanProperty
  var OC_COD : Int = 0
  // 学历
  @BeanProperty
  var EDU: Int = 0
  // 主地址标识
  @BeanProperty
  var ADR_ID: String = ""
  // 卡人主地址城市代码
  @BeanProperty
  var CIT_COD: String = ""
  // 收入
  @BeanProperty
  var INC : Float = 0
  // 婚姻状况
  @BeanProperty
  var MAR_STS : Int = 0
  //有无子女
  @BeanProperty
  var CHILD_FLAG : Int = 0
  //车辆状况
  @BeanProperty
  var VEH_FLG : String = ""
  // 现住房屋状况
  @BeanProperty
  var HOS_STS : Int = 0
  // 进件身份
  @BeanProperty
  var APP_INC_COD : Int = 0



}
