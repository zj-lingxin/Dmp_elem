package com.asto.dmp.elem.service

import java.util.Calendar

import com.asto.dmp.elem.base._
import com.asto.dmp.elem.dao.{BizDao}
import com.asto.dmp.elem.util.{BizUtils, DateUtils, FileUtils}

class CreditService extends DataSource with scala.Serializable {


  def run(): Unit = {
    try {

      FileUtils.deleteHdfsFile(Constants.OutputPath.CREDIT_TEXT)
      FileUtils.deleteHdfsFile(Constants.OutputPath.CREDIT_PARQUET)

      //月刷单额
      val monthCilckFramSales = sc.parallelize(Seq(
        ("	2015/6/13	", "	12574664669042353	", "	15453	", "	风云便当	", "	15.0 	", "	false	"),
        ("	2015/6/13	", "	12574865121584353	", "	15453	", "	风云便当	", "	24.0 	", "	true "),
        ("	2015/6/13	", "	12474664389438153	", "	15453	", "	风云便当	", "	16.0 	", "	true	"),
        ("	2015/6/3	", "	12574564579168353	", "	15453	", "	风云便当	", "	15.0 	", "	false	"),
        ("	2015/6/13	", "	12174964992731653	", "	15453	", "	风云便当	", "	16.0 	", "	true	"),
        ("	2015/6/3	", "	12174266241880553	", "	15453	", "	风云便当	", "	15.0 	", "	true	"),
        ("	2015/6/13	", "	12474064477526253	", "	15453	", "	风云便当	", "	16.0 	", "	false	"),
        ("	2015/7/13	", "	12674266136499453	", "	15453	", "	风云便当	", "	16.5 	", "	true	"),
        ("	2015/7/13	", "	12474664389438153	", "	15453	", "	风云便当	", "	16.0 	", "	true	"),
        ("	2015/7/1	", "	12574564579168353	", "	15453	", "	风云便当	", "	15.0 	", "	false	"),
        ("	2015/7/13	", "	12174964992731653	", "	15453	", "	风云便当	", "	16.0 	", "	true	"),
        ("	2015/7/13	", "	12174266241880553	", "	15453	", "	风云便当	", "	15.0 	", "	true	"),
        ("	2015/7/13	", "	12474064477526253	", "	15453	", "	风云便当	", "	16.0 	", "	false	"),
        ("	2015/7/13	", "	12674266136499453	", "	15453	", "	风云便当	", "	16.5 	", "	true	"),
        ("	2015/8/13	", "	12174866611231153	", "	15453	", "	风云便当	", "	16.0 	", "	true	"),
        ("	2015/8/13	", "	12274666824734653	", "	15453	", "	风云便当	", "	15.0 	", "	false	"),
        ("	2015/8/12	", "	12474464682199253	", "	15453	", "	风云便当	", "	15.0 	", "	true	"),
        ("	2015/8/13	", "	12174164356238453	", "	15453	", "	风云便当	", "	15.0 	", "	false	"),
        ("	2015/8/13	", "	12774265785901053	", "	15453	", "	风云便当	", "	20.0 	", "	true	"),
        ("	2015/8/12	", "	12574664669042353	", "	15453	", "	风云便当	", "	15.0 	", "	false	"),
        ("	2015/8/2	", "	12574865121584353	", "	15453	", "	风云便当	", "	24.0 	", "	true	"),
        ("	2015/8/13	", "	12474664389438153	", "	15453	", "	风云便当	", "	16.0 	", "	true	"),
        ("	2015/8/13	", "	12574564579168353	", "	15453	", "	风云便当	", "	15.0 	", "	false	"),
        ("	2015/8/13	", "	12174964992731653	", "	15453	", "	风云便当	", "	16.0 	", "	true	"),
        ("	2015/8/13	", "	12174266241880553	", "	15453	", "	风云便当	", "	15.0 	", "	true	"),
        ("	2015/8/13	", "	12474064477526253	", "	15453	", "	风云便当	", "	16.0 	", "	false	"),
        ("	2015/8/13	", "	12674266136499453	", "	15453	", "	风云便当	", "	16.5 	", "	true	"),
        ("	2015/8/13	", "	12974664290772253	", "	15453	", "	风云便当	", "	15.0 	", "	false	"),
        ("	2015/7/2	", "	12474064477526253	", "	23112	", "	吉祥馄饨	", "	16.0 	", "	false	"),
        ("	2015/7/13	", "	12674266136499453	", "	23112	", "	吉祥馄饨	", "	16.5 	", "	true	"),
        ("	2015/8/13	", "	12174866611231153	", "	23112	", "	吉祥馄饨	", "	16.0 	", "	true	"),
        ("	2015/8/13	", "	12274666824734653	", "	23112	", "	吉祥馄饨	", "	15.0 	", "	false	"),
        ("	2015/8/13	", "	12474464682199253	", "	23112	", "	吉祥馄饨	", "	15.0 	", "	true	"),
        ("	2015/8/13	", "	12174164356238453	", "	23112	", "	吉祥馄饨	", "	15.0 	", "	false	"),
        ("	2015/8/13	", "	12774265785901053	", "	23112	", "	吉祥馄饨	", "	20.0 	", "	true	"),
        ("	2015/8/2	", "	12574664669042353	", "	23112	", "	吉祥馄饨	", "	15.0 	", "	false	"),
        ("	2015/8/13	", "	12574865121584353	", "	23112	", "	吉祥馄饨	", "	24.0 	", "	true	"),
        ("	2015/8/1	", "	12474664389438153	", "	23112	", "	吉祥馄饨	", "	16.0 	", "	true	"),
        ("	2015/8/13	", "	12574564579168353	", "	23112	", "	吉祥馄饨	", "	15.0 	", "	false	"),
        ("	2015/8/4	", "	12174964992731653	", "	23112	", "	吉祥馄饨	", "	16.0 	", "	true	"),
        ("	2015/8/13	", "	12174266241880553	", "	23112	", "	吉祥馄饨	", "	15.0 	", "	true	"),
        ("	2015/8/5	", "	12474064477526253	", "	23112	", "	吉祥馄饨	", "	16.0 	", "	false	"),
        ("	2015/8/13	", "	12674266136499453	", "	23112	", "	吉祥馄饨	", "	16.5 	", "	true	"),
        ("	2015/8/13	", "	12974664290772253	", "	23112	", "	吉祥馄饨	", "	15.0 	", "	false	")
      )).filter(t => t._6.trim == "true"). //过滤出刷单的数据
        map(t => ((DateUtils.cutYearMonth(t._1.trim), t._3.trim), t._5.trim.toDouble)). //整理字段((2015/7,15453),16.5) 、 ((2015/7,15453),16.0)、((2015/7,15453),16.0) ...
        groupByKey(). //((2015/7,15453),CompactBuffer(16.5, 16.0, 16.0, 15.0, 16.5))
        map(t => (t._1, t._2.sum)) //((2015/7,15453),80.0)

      //月营业额
      val monthSales = BizDao.getOrderProps(SQL().setSelect("order_date,shop_id,order_money")).
        map(a => ((DateUtils.cutYearMonth(a(0).toString), a(1).toString), a(2).toString.trim.toDouble)). //((2015/7,15453),14.0)
        groupByKey(). //((2015/7,15453),CompactBuffer(16.5, 16.0, 16.0, 15.0, 16.5,...))
        map(t => (t._1, t._2.sum)) //((2015/7,15453),37548.0)

      //月净营业额 = 月营业额 - 月刷单额
      val monthNetSales = monthSales.leftOuterJoin(monthCilckFramSales). //((2015/7,15453),(37548.0,Some(80.0)))
        map(t => (t._1, t._2._1 - t._2._2.getOrElse(0D))) //((2015/7,15453),37468.0)

      //得到近6个月的月份(当前月大于等于15号，算一个月；忽略2月)
      val fiveMonths = BizUtils.getLastMonths(6)
      //最近一个月不需要，去除。
      fiveMonths.remove(0, 1)

      //营业额加权环比增长率的权重值
      val weightList = List(0.3, 0.2, 0.2, 0.15, 0.15)
      //月份和权重值对应关系

      val monthsAndWeights = fiveMonths.zip(weightList)

      //月日净营业额 = 月净营业额/每个月的天数
      val avgDayInMonthNetSales = monthNetSales.
        filter(t => fiveMonths.contains(t._1._1)).
        map(t => (t._1, CreditService.getAvgSales(t._1._1, t._2)))

      val RiAddOne = avgDayInMonthNetSales.
        map(t => (CreditService.minusOneMonth(t._1._1), t._2)).
        filter(t => fiveMonths.contains(t._1))

      val monthNetSalesNeeds = avgDayInMonthNetSales.
        filter(t => fiveMonths.contains(t._1._1)).
        map(t => (t._1._1, (t._1._2, t._2))).
        leftOuterJoin(RiAddOne)

      //营业额加权环比增长率
      val salesRateWeighting = sc.parallelize(monthsAndWeights).
        leftOuterJoin(monthNetSalesNeeds).
        filter(t => t._2._2.isDefined).
        map(t => (t._1, t._2._1, t._2._2.get._1._1, t._2._2.get._1._2, t._2._2.get._2.getOrElse(0D))). //(2015/7,0.15,15453,1208.6451612903227,748.5967741935484)
        map(t => (t._3, t._2 * t._5 / t._4)). // (15453,0.12387370556207963)
        reduceByKey(_ + _)

      //准入规则中的输出
      //餐厅ID(ShopID)、shopName(餐厅名称)、shopDuration(平台连续经营时间)、dayAverageSales(日均净营业额)、saleRateInThreeMonths(基于3个月的营业额增长率)、saleRateInOneMonths(基于1个月的营业额增长率)、fakedSalesRate(刷单率)、ifAccess(是否准入)
      val dayAverageSales = sc.parallelize(Seq(
        ("	15453	", "	风云便当	", "14", "2000.0", "0.03", "0.02", "0.01", "true"),
        ("	23112	", "	吉祥馄饨	", "9", "2000.0", "0.02", "0.03", "0.03", "true")
      )).map(t => (t._1.toString.trim, t._4.toString.trim.toDouble))

      val beta = salesRateWeighting.leftOuterJoin(dayAverageSales). //(15453,(0.2800160158853955,Some(1000.0)))
        map(t => (t._1, t._2._1.toDouble, t._2._2.getOrElse("0").toString.toDouble)).
        map(t => (t._1,t._2,t._3,CreditService.getBeta(t._2, t._3))) //(15453,0.2800160158853955,2000.0,0.2)

      //计算近12个月日营业额均值

      /*
            val sql = SQL("order_id, shop_id, shop_name, custom_id, custom_name", "order_id = 12254695004719553 ")
            BizDao.getOrderProps(sql).map(t=>(t(0),t(1))).take(10).foreach(println)*/

      /*  val sql2 = SQL("order_id, shop_id, shop_name, custom_id, custom_name")
        BizDao.getOrderProps(sql2).take(10).foreach(println)

        val sql3 = SQL()
        BizDao.getOrderProps(sql3).take(10).foreach(println)

        BizDao.getOrderProps().take(10).foreach(println)
*/
      //val sc.parallelize()

      /**
      val result = totalSales.map(t => (t._1, (t._2, t._2 / 12 * 1.5))).leftOuterJoin(tmpResult).map( t => (t._1, t._2._1._1, t._2._2.get._1, t._2._2.get._2, t._2._2.get._3, t._2._2.get._4,
        t._2._2.get._5, t._2._1._2, t._2._2.get._6, t._2._2.get._7, t._2._2.get._10, t._2._2.get._11, Array(t._2._1._2, t._2._2.get._8).min))
      result.toDF("shop_id", "前12个月销售总额", "resultSales", "β", "行业类型", "得分", "resultSales*β/12*授信月份数",
        "前12个月销售总额/12*1.5", "行业风险限额", "产品限额", "刷单比率", "退款率", "授信额度").write.parquet(Constants.OutputPath.CREDIT_PARQUET)
      result.map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile(Constants.OutputPath.CREDIT_TEXT)
        * */
    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.CREDIT_SUBJECT, t)
    }
  }
}

object CreditService {
  private def getAvgSales(strDate: String, monthSales: Double) = monthSales / BizUtils.getDaysNumInMonth(strDate, "yyyy/M")

  private def minusOneMonth(strDate: String): String = {
    val cal = DateUtils.strToCalendar(strDate, "yyyy/M")
    cal.add(Calendar.MONTH, -1)
    DateUtils.getStrDate(cal, "yyyy/M")
  }

  //授信额度上限(单位：元)
  val loanCeiling = 500000

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

}
