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

    val LOG_WRAPPER = "######################"
  }

  /** 输入文件路径 **/
  object InputPath {
    //hdfs中表的字段的分隔符
    val SEPARATOR = "@#@"
    private val DIR        = s"${App.HADOOP_DIR}/input"
    val ORDER              = s"$DIR/order/*"
  }

  /** 中间文件路径 **/
  object MiddlePath {
    val SEPARATOR = ","
    private val DIR        = s"${App.HADOOP_DIR}/middle"
    val BETA               = s"$DIR/beta/*"
    val INDUSTRY_RELATION  = s"$DIR/industry_relation" //这里不需要“/*”
  }

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = ","
    private val TODAY    = DateUtils.getStrDate("yyyyMM/dd")
    private val DIR      = s"${App.HADOOP_DIR}/output"
    //刷单结果路径
    val FRAUD_PARQUET    = s"$DIR/parquet/$TODAY/fraud"
    //得分结果路径
    val SCORE_TEXT       = s"$DIR/text/$TODAY/score"
    //授信结果路径
    val CREDIT_TEXT      = s"$DIR/text/$TODAY/credit"
    val CREDIT_PARQUET   = s"$DIR/parquet/$TODAY/credit"
  }

  /** 表的模式 **/
  object Schema {
    //订单ID, 餐厅ID, 餐厅Id, 餐厅名称, 订单详情json, 下单客户ID, 客户名称, 城市ID, 订单额, 退款状态, 下单时间, 手机号, 下单配送地址, 下单经纬度d
    val ORDER = "order_date:String, order_id:String, shop_id:String, shop_name:String, order_details:String, " +
      "custom_id:String, custom_name:String, city_id:String, order_money:String, refund_status:String, " +
      "place_order_time:String, custom_mobile:String, delivery_address:String, lng_lat:String"
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
    val CREDIT_SUBJECT  = s"${App.CHINESE_NAME}-授信规则 项目出现异常，请尽快查明原因！"
    val APPROVE_SUBJECT = s"${App.CHINESE_NAME}-准入规则结果集写入失败，请尽快查明原因！"
  }

}
