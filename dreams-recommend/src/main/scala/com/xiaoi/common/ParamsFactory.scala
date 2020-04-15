package com.xiaoi.common

object ParamsFactory {

  case class Params(
                     //basic data
                     crm_data_cleaned_input_path: String = "",
                     room_data_input_path: String = "",

                     //hotelETL
                     limit_month_enabled: Boolean = true,
                     limit_month_input: Int = 6,
                     observe_date_enabled: Boolean = true,
                     observe_date: String = "2018-01-01",
                     merged_output_path: String = "file:///xiaoi/data/blend/",
                     user_attribute_table: String = "user_attribute",

                     //uLabelBase
                     small_category_input_path: String = "file:///xiaoi/crm/data/step_2/small_category_info",
                     calendar_input_path: String = "file:///xiaoi/ming-recommend/data/calendar/2017",

                     //uLabelValue
                     filtered_output_path: String = "file:///xiaoi/ming-recommend/data/step_205/filtered_output_path",

                     //iAttr
                     start_date: String = "2017-05-31",

                     //iLabel
                     hot_sale_threshold: Int = 3,
                     no_exception_input_path: String = "file:///xiaoi/crm/data/step_1/exception_handled",
                     end_date: String = "2017-07-01",
                      interval_in_days: Int = 30,

                     //mysql
                     db_url: String = "jdbc:mysql://122.226.240.158:3306/recommender",
                     db_user_name: String = "meizu",
                     db_password: String = "123",
                     db_driver: String = "com.mysql.jdbc.Driver",
                     label_table: String = "user_label",
                     attribute_table: String = "u_basic_attr"

                   )

}
