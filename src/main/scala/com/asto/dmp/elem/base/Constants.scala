package com.asto.dmp.elem.base

import com.asto.dmp.elem.util.DateUtils

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    //spark UI界面显示的名称
    val SPARK_UI_APP_NAME  = "elem_statistics"
    //项目的中文名称
    val CHINESE_NAME       = "饿了么"
    //项目数据存放的Hadoop的目录
    val HADOOP_DIR         = "hdfs://appcluster/elem/"

    val LOG_WRAPPER        = "######################"
  }

  /** 输入文件路径 **/
  object InputPath {
    //hdfs中表的字段的分隔符
    val SEPARATOR = "\t"
    private val DIR        = s"${App.HADOOP_DIR}/input"
    val ORDER              = s"$DIR/order/*"
  }

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = ","
    private val TODAY        = DateUtils.getStrDate("yyyyMM/dd")
    private val DIR          = s"${App.HADOOP_DIR}/output"

    //反欺诈结果路径
    val ANTI_FRAUD_FAKED_INFO_TEXT      = s"$DIR/text/$TODAY/fraud/fakedInfo"
    val ANTI_FRAUD_FAKED_RATE_TEXT      = s"$DIR/text/$TODAY/fraud/fakedRate"
    //授信结果路径
    val CREDIT_TEXT          = s"$DIR/text/$TODAY/credit"
    //授信结果路径
    val ACCESS_TEXT          = s"$DIR/text/$TODAY/access"
  }

  /** 表的模式 **/
  object Schema {
    //订单表：订单ID, 餐厅ID, 餐厅Id, 餐厅名称, 下单客户ID, 客户名称, 城市ID, 订单额, 退款状态, 下单时间, 手机号, 下单配送地址, 下单经纬度d
    val ORDER = "order_date:String, order_id:String, shop_id:String, shop_name:String, " +
      "custom_id:String, custom_name:String, city_id:String, order_money:String, refund_status:String, " +
      "place_order_time:String, custom_mobile:String, delivery_address:String, lng_lat:String"

    //反欺诈结果输出1：订单ID, 订单日期, 餐厅ID ,餐厅名称 ,下单客户ID	,下单时间	,订单额 ,刷单指标值1	,刷单指标值2,	刷单指标值3,	刷单指标值4,	刷单指标值5,	是否刷单
    val FAKED_INFO = "order_id:String,order_date:String,shop_id:String,shop_name:String,custom_id:String,place_order_time:String,order_money:String,fqz1:String,fqz2:String,fqz3:String,fqz4:String,fqz5:String,is_faked:String"

    //反欺诈结果输出2：餐厅ID,餐厅名称,近6个月刷单金额	,近6个月总营业额	,刷单率
    val FAKED_RATE = "shop_id:String,shop_name:String,last_six_months_faked_sales:String,last_six_months_total_sales:String,faked_rate:String"

    //准入结果输出：餐厅ID,餐厅名称,平台连续经营时间,日均净营业额,基于3个月的营业额增长率,基于1个月的营业额增长率,刷单率,是否准入)
    val ACCESS_INFO = "shop_id:String,shop_name:String,shop_duration:String,day_average_sales:String,sale_rate_in_three_months:String,sale_rate_in_one_months:String,faked_sales_rate:String,is_access:String"
  }

  /** 邮件发送功能相关常量 **/
  object Mail {
    //以下参数与prop.properties中的参数一致。
    val TO            = "zhengc@asto-inc.com"
    val FROM          = "dmp_notice@asto-inc.com"
    val PASSWORD      = "astodmp2015"
    val SMTPHOST      = "smtp.qq.com"
    val SUBJECT       = s"“${App.CHINESE_NAME}”项目出异常了！"
    val CC            = ""
    val BCC           = ""
    val TO_FRAUD      = "fengtt@asto-inc.com"
    val TO_SCORE      = "yewb@asto-inc.com"
    val TO_ACCESS     = "lij@asto-inc.com"
    val TO_CREDIT     = "lingx@asto-inc.com"
    val TO_LOAN_AFTER = "liuw@asto-inc.com"

    //以下参数prop.properties中没有， MAIL_CREDIT_SUBJECT是授信规则模型出问题时邮件的主题
    val CREDIT_SUBJECT      = s"${App.CHINESE_NAME}-授信规则结果集写入失败，请尽快查明原因！"
    val ACCESS_SUBJECT      = s"${App.CHINESE_NAME}-准入规则结果集写入失败，请尽快查明原因！"
    val ANTI_FRAUD_SUBJECT  = s"${App.CHINESE_NAME}-反欺诈规则结果集写入失败，请尽快查明原因！"
    val LOAN_AFTER_SUBJECT  = s"${App.CHINESE_NAME}-贷后预警规则结果集写入失败，请尽快查明原因！"
  }

}
