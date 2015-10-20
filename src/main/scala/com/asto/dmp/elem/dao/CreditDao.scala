package com.asto.dmp.elem.dao

import com.asto.dmp.elem.base.{Constants, BaseContext}

class CreditDao extends scala.Serializable  {
  private val sqlContext = BaseContext.getSqlContext
  import sqlContext.implicits._

  //定义totalSales = 前12个月销售总额。得到一个数组 Array[(Long, Double)]，数组的元素形如(9631,289431.5)，其中9631是shop_id,289431.5是近12个月的销售总额
  def totalSales = {
    val tradeViewSales = sqlContext.sql("select shop_id,gmt_target, pay_money from tbcm_trade_over_view ").
      map(a => (a(0).toString, a(1).toString, a(2).toString.toDouble))
    val tradeSales = sqlContext.sql("select shop_id,gmt_target, pay_money from tbcm_trade").
      map(a => (a(0).toString, a(1).toString, a(2).toString.toDouble))

    tradeViewSales.union(tradeSales).distinct().toDF("shop_id", "gmt_target", "pay_money").registerTempTable("union_sales_table")
    val totalSales = sqlContext.sql("select shop_id, sum(pay_money) from union_sales_table group by shop_id ").map(a => (a(0).toString, a(1).toString.toDouble))
    totalSales
  }

  def refundRate =
    sqlContext.sql("select shop_id,avg(refund_rate) from refund_info group by shop_id ").map(t => (t(0).toString, t(1)))

  /** 获取店铺的行业类型 类型：(property_uuid, 行业类型) **/
  def shopInfo =
    sqlContext.sql("select property_uuid, industry_type from property_shop ").map(a => (a(0).toString, a(1).toString))

  //调整参数项: β = β（原）* 1.25
  /** 获取β值 类型：(industry_type, sales_upper_limit, sales_lower_limit, score_upper_limit, score_lower_limit, beta_score) **/
  def betaInfo =
    sqlContext.sql("select * from beta ").map(t => (t(0).toString, (t(1).toString.toInt, t(2).toString.toInt, t(3).toString.toInt, t(4).toString.toInt, t(5).toString.toDouble * 1.25)))

/*  def shuadanRate = {
    //授信结果路径
    sqlContext.read.parquet(Constants.OutputPath.FRAUD_PARQUET).registerTempTable("shuadan")
    sqlContext.sql("select * from shuadan").map(t => Pair[String, Double](t(0).toString, if (t(1) == null) 0D else t(1).toString.toDouble))
  }*/
}
