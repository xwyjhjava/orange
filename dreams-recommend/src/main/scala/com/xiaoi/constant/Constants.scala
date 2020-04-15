package com.xiaoi.constant

/**
  * Constants Interface
  * Created by josh.ye on 6/19/17.
  */
object Constants {
  /**
    * Project Config
    */
  val DATABASE = "database"
  val JDBC_DRIVER = "jdbc.driver"
  val JDBC_DATASOURCE_SIZE = "jdbc.datasource.size"
  val JDBC_URL = "jdbc.url"
  val JDBC_USER = "jdbc.user"
  val JDBC_PASSWORD = "jdbc.password"
  val JDBC_URL_PROD = "jdbc.url.prod"
  val JDBC_USER_PROD = "jdbc.user.prod"
  val JDBC_PASSWORD_PROD = "jdbc.password.prod"

  /**
    * ETL
    */
  val IMPORT_DAY = "import"
  val OLD_IMPORT = "old_import"

  /**
    * Spark Job
    */
  val SPARK_LOCAL = "spark.local"
  val SPARK_MASTER_URL = "spark://master:7077"

  /**
    * Special Character
    */
  val STOP_WORDS = "stop_words"

  /**Fields Length **/
  val FIELDS_LENGTH = "fields_length"

  /**
    * Data Cleaned Fields
    */
  val FIELD_UID_ = "uid_"
  val FIELD_SLDAT_ = "sldat_"
  val FIELD_VIPNO_ = "vipno_"
  val FIELD_PRODNO_ = "prodno_"
  val FIELD_PLUNAME_ = "pluname_"
  val FIELD_DPTNO_ = "dptno_"
  val FIELD_DPTNAME_ = "dptname_"
  val FIELD_BNDNO_ = "bndno_"
  val FIELD_BNDNAME_ = "bndname_"
  val FIELD_QTY_ = "qty_"
  val FIELD_AMT_ = "amt_"
  val FIELD_VIP_GENDER_ = "vip_gender_"
  val FIELD_VIP_BIRTHDAY_ = "vip_birthday_"
  val FIELD_VIP_CREATED_DATE_ = "vip_created_date_"

  /**
    * data_cols Fields
    */
  val FIELD_UID = "uid"
  val FIELD_SLDAT = "sldat"
  val FIELD_VIPID = "vipid"
  val FIELD_PRODNO = "prodno"
  val FIELD_PLUNAME = "pluname"
  val FIELD_DPTNO = "dptno"
  val FIELD_DPTNAME = "dptname"
  val FIELD_BNDNO = "bndno"
  val FIELD_BNDNAME = "bndname"
  val FIELD_QTY = "qty"
  val FIELD_AMT = "amt"
  val FIELD_VIP_GENDER = "vip_gender"
  val FIELD_VIP_BIRTHDAY = "vip_birthday"
  val FIELD_VIP_CREATED_DATE = "vip_created_date"
  val FIELD_ACTION_TYPE = "action_type"
  val FIELD_MODEL_ID = "model_id"

  /**
    * Step_3 Index of Table Name
    */
  val INDEX_BASIC_INFO = "basic_info"
  val INDEX_ITEM_DETAIL = "item_detail"
  val INDEX_USER_DETAIL = "user_detail"
  val INDEX_USER_UID = "user_uid"
  val INDEX_UID_SLDAT = "uid_sldat"
  val INDEX_UID_ITEM = "uid_item"
  val INDEX_DATA_SIMPLIFIES = "data_simplifies"
  val INDEX_USER_BASKET_HAS_ITEM_LIST = "user_basket_has_item_list"
  val INDEX_USER_BASKET_MONEY = "user_basket_money"
  val INDEX_USER_TOTAL = "user_total"
  val INDEX_ITEM_TOTAL = "item_total"
  val INDEX_ITEM_TOP100_BY_LOG_COUNT = "item_top100_by_log_count"
  val INDEX_ITEM_TOP100_BY_USER_COUNT = "item_top100_by_user_count"
  val INDEX_CLASS_TOTAL = "class_total"
  val INDEX_CLASS_TOP10 = "class_top10"

  /**
    * FrequencyUtil
    */
  val YEAR_PART = Array(0,365/2,365,365*2,365*3)
  val MONTH_PART = Array(0,30,60,90,120,150,180)

  /**
    * step_4
    */
  val YEAR_CHOOSE = 365*3
  val MONTH_CHOOSE = 30*6
  val ONE_MONTH = 30
  val USER_ITEM_DAYS = 15

}