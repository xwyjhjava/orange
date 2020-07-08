package com.xiaoi.bank

/**
 * @Package com.xiaoi.bank
 * @author ming
 * @date 2020/7/6 18:05
 * @version V1.0
 * @description TODO
 */
object BankEntity{


  // 用户表
  case class Tb_User(
                    //编号
                    NBR : String,
                    // 年龄
                    SEX : Int,
                    // 性别
                    AGE : Int,
                    // 国籍
                    CITY_ID: Int,
                    // 民族
                    ETH_GRP : Int,
                    // 行业类别
                    COR_TYPE : Int,
                    // 岗位
//                    TIT_NAME : String,
                    // 职业等级
                    CC_COD : Int,
                    // 职称
                    OC_COD : Int,
                    // 学历
                    EDU: Int,
                    // 主地址标识
                    ADR_ID: String,
                    // 卡人主地址城市代码
                    CIT_COD: String,
                    // 收入
                    INC : Float,
                    // 婚姻状况
                    MAR_STS : Int,
                    //有无子女
                    CHILD_FLAG : Int,
                    //车辆状况
                    VEH_FLG : String,
                    // 现住房屋状况
                    HOS_STS : Int,
                    // 进件身份
                    APP_INC_COD : Int
                    // 最新申请书时间
//                    APP_DATE: String

                    )


  // 订单表
  case class Tb_Order(
                     // 客户号
                     NBR : String,
                     // 卡人账单城市
//                     BILL_CITY: String,
                     // 电话营销成交日期
                     SALE_DATE : String,
                     // 保险公司代码
                     MERCH_COD: String,
                     // 保险公司名称
                     MERCH_NAME : String,
                     // 产品号
                     PROD_NO: String,
                     // 产品类型
                     TYPE : String,
                     // 保额(万元)
                     COVERAGE: Float,
                     // 是否免费险
//                     FREE_FLAG : String,
                     // 保险时长单位
                     INSURE_TERM_TY: Int,
                     // 保险时长数值
                     INSURE_TERM: Float,
                     // 缴费时长单位
                     METHDD_TERM_TY : Int,
                     // 缴费时长数值
                     METHDD_TERM : Float,
                     // 缴别
                     PAYT_PERQ : Int,
                     // 每期保费
                     PERM_FEE : Float,
                     // 本年度保费金额
                     YEAR_FEE : Float,
                     // 总保费
                     TOT_FEE : Float,
                     // 累计已缴保费金额
                     PAYED_PERM: Float,
                     // 被保人和投保人关系
                     PAYED_INSURED_SAME_F : String,
                     // 被保人年龄
                     INSURE_AGE : Int


                     )


}
