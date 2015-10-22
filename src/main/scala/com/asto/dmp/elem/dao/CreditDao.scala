package com.asto.dmp.elem.dao

import java.util.Calendar
import com.asto.dmp.elem.base.{Contexts, SQL}
import com.asto.dmp.elem.util.{BizUtils, DateUtils}

object CreditDao extends scala.Serializable  {
  /**
   * 授信额度上限(单位：元)
   */
  val loanCeiling = 500000

  /**
   * 计算刷单率
   */
  def getFakedRate = {
    BizDao.getFakedRateProps(SQL().setSelect("shop_id,shop_name,faked_rate"))
      .map(a => (a(0).toString, (a(1).toString, a(2).toString.toDouble))) //(15453,(风云便当,0.13))
  }

  /**
   * 计算近12个月日营业额均值
   */
  def getLast12MonthsAvgSales = {
    val last12MothsList = BizUtils.getLastMonths(12,"yyyy/M")
    BizDao.getOrderProps(SQL().setSelect("order_date,shop_id,order_money"))
      .map(a => ((DateUtils.cutYearMonth(a(0).toString), a(1)), a(2).toString.toDouble))
      .filter(t => last12MothsList.contains(t._1._1)) //((2014/10,15453),13.0)
      .groupByKey() //((2014/10,15453),CompactBuffer(7.0, 12.0, 13.0, 11,...))
      .map(t => (t._1, t._2.sum)) //((2014/10,15453),113392.0)
      .map(t => (t._1, CreditDao.getAvgSales(t._1._1, t._2)))
      .map(t => (t._1._2, t._2))
      .reduceByKey(_ + _) //12个月日均销售总额 (15453,38746.96612903226)
      .map(t => (t._1.toString, t._2 / 12)) //近12个月日均营业额均值 (15453,3228.913844086022)
  }

  /**
   * 计算营业额加权环比增长率
   */
  def getSalesRateWeighting = {
    //月刷单额
    val monthFakedSales = BizDao.getFakedInfoProps(SQL().setSelect("order_date,order_id,shop_id,shop_name,order_money,is_faked")) // (订单日期:"2015/8/13", 订单ID:"12974664290772253", 餐厅ID:"15453", 餐厅名称:"风云便当", 订单额:"15.0", 是否刷单:"false")
      .filter(_(5).toString.toBoolean) //过滤出刷单的数据
      .map(a => ((DateUtils.cutYearMonth(a(0).toString), a(2).toString), a(4).toString.toDouble)) //((2015/7,15453),16.5)
      .groupByKey() //((2015/7,15453),CompactBuffer(16.5, 16.0, 16.0, 15.0, 16.5))
      .map(t => (t._1, t._2.sum)) //((2015/7,15453),80.0)

    //月营业额
    val monthSales = BizDao.getOrderProps(SQL().setSelect("order_date,shop_id,order_money"))
      .map(a => ((DateUtils.cutYearMonth(a(0).toString), a(1).toString), a(2).toString.trim.toDouble)) //((2015/7,15453),14.0)
      .groupByKey() //((2015/7,15453),CompactBuffer(16.5, 16.0, 16.0, 15.0, 16.5,...))
      .map(t => (t._1, t._2.sum)) //((2015/7,15453),37548.0)

    //月净营业额 = 月营业额 - 月刷单额
    val monthNetSales = monthSales.leftOuterJoin(monthFakedSales) //((2015/7,15453),(37548.0,Some(80.0)))
      .map(t => (t._1, t._2._1 - t._2._2.getOrElse(0D))) //((2015/7,15453),37468.0)

    //得到近6个月的月份(当前月大于等于15号，算一个月；忽略2月)
    val fiveMonths = BizUtils.getLastMonths(6,"yyyy/M")

    //最近一个月不需要，去除。
    fiveMonths.remove(0, 1)

    //营业额加权环比增长率的权重值
    val weightList = List(0.3, 0.2, 0.2, 0.15, 0.15)

    //月份和权重值对应关系
    val monthsAndWeights = fiveMonths.zip(weightList)

    //月日净营业额 = 月净营业额/每个月的天数
    val avgDayInMonthNetSales = monthNetSales
      .filter(t => fiveMonths.contains(t._1._1))
      .map(t => (t._1, getAvgSales(t._1._1, t._2)))

    val RiAddOne = avgDayInMonthNetSales
      .map(t => (minusOneMonth(t._1._1), t._2))
      .filter(t => fiveMonths.contains(t._1))

    val monthNetSalesNeeds = avgDayInMonthNetSales
      .filter(t => fiveMonths.contains(t._1._1))
      .map(t => (t._1._1, (t._1._2, t._2)))
      .leftOuterJoin(RiAddOne)

    //营业额加权环比增长率
    Contexts.getSparkContext.parallelize(monthsAndWeights)
      .leftOuterJoin(monthNetSalesNeeds)
      .filter(t => t._2._2.isDefined)
      .map(t => (t._1, t._2._1, t._2._2.get._1._1, t._2._2.get._1._2, t._2._2.get._2.getOrElse(0D))) //(2015/7,0.15,15453,1208.6451612903227,748.5967741935484)
      .map(t => (t._3, t._2 * t._5 / t._4)) // (15453,0.12387370556207963)
      .reduceByKey(_ + _)
  }

  /**
   * 贷款倍率: 该值由营业额加权环比增长率（SalesRateWeighting）和准入规则中定义的日均净营业额（DayAverageSales）这两个参数决定。
   */
  def getBetaInfo = {
    //准入规则中的输出
    val dayAverageSales = BizDao.getAccessInfoProps(SQL().setSelect("shop_id,day_average_sales,is_access")).map(a => (a(0).toString,(a(1).toString.toDouble,a(2)))) //(shop_id,day_average_sales)
    getSalesRateWeighting.leftOuterJoin(dayAverageSales) //(15453,(0.2800160158853955,Some((1000.0,true))))
      .map(t => (t._1, t._2._1.toDouble, t._2._2.getOrElse((0,false))._1.toString.toDouble, t._2._2.getOrElse((0,false))._2))
      .map(t => (t._1, (t._2, t._3, getBeta(t._2, t._3),t._4))) //(15453,(0.2800160158853955,2000.0,0.18,true))
  }

  private def getBeta(salesRateWeighting: Double, dayAverageSales: Double): Double = {
    if (salesRateWeighting > 2)
      getBetaByAvgSalAndRow(dayAverageSales, Array(1.50, 1.30, 1.20, 1.00, 0.90))
    else if (salesRateWeighting > 1.5 && salesRateWeighting <= 2)
      getBetaByAvgSalAndRow(dayAverageSales, Array(1.30, 1.10, 1.00, 0.80, 0.70))
    else if (salesRateWeighting > 1.2 && salesRateWeighting <= 1.5)
      getBetaByAvgSalAndRow(dayAverageSales, Array(1.20, 1.00, 0.90, 0.70, 0.60))
    else if (salesRateWeighting > 1 && salesRateWeighting <= 1.2)
      getBetaByAvgSalAndRow(dayAverageSales, Array(1.00, 0.90, 0.80, 0.60, 0.50))
    else if (salesRateWeighting > 0.8 && salesRateWeighting <= 1)
      getBetaByAvgSalAndRow(dayAverageSales, Array(0.90, 0.70, 0.60, 0.40, 0.20))
    else
      0.2
  }

  private def getBetaByAvgSalAndRow(dayAverageSales: Double, betaRow: Array[Double]): Double = {
    if (dayAverageSales > 5000)
      betaRow(0)
    else if (dayAverageSales > 4000 && dayAverageSales <= 5000)
      betaRow(1)
    else if (dayAverageSales > 3000 && dayAverageSales <= 4000)
      betaRow(2)
    else if (dayAverageSales > 2000 && dayAverageSales <= 3000)
      betaRow(3)
    else if (dayAverageSales > 1000 && dayAverageSales <= 2000)
      betaRow(4)
    else
      0D
  }

  private def getAvgSales(strDate: String, monthSales: Double) = monthSales / BizUtils.getDaysNumInMonth(strDate, "yyyy/M")

  private def minusOneMonth(strDate: String): String = {
    val cal = DateUtils.strToCalendar(strDate, "yyyy/M")
    cal.add(Calendar.MONTH, -1)
    DateUtils.getStrDate(cal, "yyyy/M")
  }
}
