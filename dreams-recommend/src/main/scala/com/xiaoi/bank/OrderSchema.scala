package com.xiaoi.bank

import scala.beans.BeanProperty

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/9 11:52
 * @version V1.0
 * @description TODO
 */
class OrderSchema {

  // 客户号
  @BeanProperty
  var NBR : String = ""
  // 电话营销成交日期
  @BeanProperty
  var SALE_DATE : String = ""
  // 保险公司代码
  @BeanProperty
  var MERCH_COD: String = ""
  // 保险公司名称
  @BeanProperty
  var MERCH_NAME : String = ""
  // 产品号
  @BeanProperty
  var PROD_NO: String = ""
  // 产品类型
  @BeanProperty
  var TYPE : String = ""
  // 保额(万元)
  @BeanProperty
  var COVERAGE: Float = 0
  // 保险时长单位
  @BeanProperty
  var INSURE_TERM_TY: Int = 0
  // 保险时长数值
  @BeanProperty
  var INSURE_TERM: Float = 0
  // 缴费时长单位
  @BeanProperty
  var METHDD_TERM_TY : Int = 0
  // 缴费时长数值
  @BeanProperty
  var METHDD_TERM : Float = 0
  // 缴别
  @BeanProperty
  var PAYT_PERQ : Int = 0
  // 每期保费
  @BeanProperty
  var PERM_FEE : Float = 0
  // 本年度保费金额
  @BeanProperty
  var YEAR_FEE : Float = 0
  // 总保费
  @BeanProperty
  var TOT_FEE : Float = 0
  // 累计已缴保费金额
//  @BeanProperty
//  var PAYED_PERM: Float = 0
  // 被保人和投保人关系
  @BeanProperty
  var PAYED_INSURED_SAME_F : String = ""
  // 被保人年龄
  @BeanProperty
  var INSURE_AGE : Int = 0

}
