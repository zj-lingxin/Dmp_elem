package com.asto.dmp.elem.dao

import java.util.Calendar
import com.asto.dmp.elem.base.{Constants, SQL}
import com.asto.dmp.elem.util.{BizUtils, DateUtils}

/**
 * 基于商户近6月的营业额，判定该商户是否有贷款准入资格。
 * 该规则由5个指标组成，只有所有指标均满足其对应的贷前阈值，才能获得贷款准入资格。
 * 名称说明：
 * "日营业额"指该日所有订单额之和
 * "日刷单金额"指该日所有刷单的订单总金额
 * "日净营业额"指该日营业额与该日刷单金额之差
 */
object AccessDao {
  private val lastSixMonthsDayAverageSales = BizUtils.lastMonthsDayAverageSales(6)

  /**
   * 平台连续经营时间(ShopDuration): 在饿了么平台连续有交易额的自然月月份数
   * 注：1. 扣除特殊月
   * 2. 采集数据截止日期为15号之后(包含15号)，则包含当前月，否则从上个自然月算起。
   * 贷前阈值: ShopDuration ≥ 6
   * 返回字段：(店铺ID,平台连续经营时间)
   */
  def getShopDuration = {
    BizDao.getOrderProps(SQL().setSelect("shop_id,order_date"))
      .map(a => (a(0).toString, DateUtils.cutYearMonth(a(1).toString)))
      .distinct().groupByKey().map(t => (t._1, getDuration(t._2.toList)))
  }

  /**
   * 日均净营业额(DayAverageSales): 近6个月 日均净营业额之和 / 6
   * 贷前阈值: DayAverageSales ≥ 1000元
   * 返回字段:(店铺ID,近6个月日均净营业额)
   */
  def getDayAverageSales = lastSixMonthsDayAverageSales


  /**
   * 基于3个月的营业额增长率(SaleRateInThreeMonths): 近3个月日净营业额均值 / 近6个月日净营业额均值
   * 贷前阈值: SaleRateInThreeMonths ≥ 1
   * 返回字段:(店铺ID,基于3个月的营业额增长率)
   */
  def getSaleRateInThreeMonths = {
    BizUtils.lastMonthsDayAverageSales(3).leftOuterJoin(lastSixMonthsDayAverageSales) // (15453,(238.93896569380442,Some(1844.7884792626726)))
      .filter(t => t._2._2.isDefined && t._2._2.get > 0) //过滤出符合条件的分母
      .map(t => (t._1, t._2._1 / t._2._2.get))
  }

  /**
   * 基于1个月的营业额增长率(SaleRateInOneMonths): 近1个月日净营业额均值 / 近6个月日净营业额均值
   * 贷前阈值: SaleRateInOneMonths ≥ 1
   * 返回字段:(店铺ID,基于1个月的营业额增长率)
   */
  def getSaleRateInOneMonths = {
    BizUtils.lastMonthsDayAverageSales(1).leftOuterJoin(lastSixMonthsDayAverageSales)
      .filter(t => t._2._2.isDefined && t._2._2.get > 0) //过滤出符合条件的分母
      .map(t => (t._1, t._2._1 / t._2._2.get))
  }

  /**
   * 刷单率(FakedSalesRate):	近6个月总刷单金额 / 近6个月总营业额(不折算)
   * 贷前阈值: FakedSalesRate ≤ 20%
   * 返回字段：(店铺ID,(店铺名称,刷单率))
   */
  def getFakedSalesRate = {
    BizDao.getFakedRateProps(SQL().setSelect("shop_id,shop_name,faked_rate"))
      .map(a => (a(0).toString, (a(1).toString, a(2).toString.toDouble))) //(15453,(风云便当,0.13))
  }

  private def backMonths(strDate: String, m: Int, formatText: String): String = {
    val cal = DateUtils.strToCalendar(strDate, formatText)
    cal.add(Calendar.MONTH, -m)
    DateUtils.getStrDate(cal, formatText)
  }

  private def getDuration(list: List[String]) = {
    val sortedList = list.map(DateUtils.strToStr(_, Constants.App.YEAR_MONTH_FORMAT, "yyyy/MM")).sorted.reverse
    var yearAndMonth = BizUtils.getInitYearAndMonth("yyyy/MM")
    var duration = 0
    var end = false
    sortedList.toStream.takeWhile(_ => !end).foreach { dateInRecords =>
      if (dateInRecords == yearAndMonth) {
        duration += 1
        if (yearAndMonth.contains("/03"))
          yearAndMonth = backMonths(yearAndMonth, 2, "yyyy/MM")
        else
          yearAndMonth = backMonths(yearAndMonth, 1, "yyyy/MM")
      } else if (dateInRecords < yearAndMonth) {
        end = true
      }
      //如果dateInRecords > yearAndValue，什么都不做
    }
    duration
  }

}
