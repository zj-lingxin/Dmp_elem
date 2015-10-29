package com.asto.dmp.elem.util

import java.util.Calendar
import com.asto.dmp.elem.base.{Constants, SQL}
import com.asto.dmp.elem.dao.BizDao
import org.apache.spark.rdd.RDD
import scala.collection._

/**
 * 该类中定义的是跟业务相关的一些共用方法。这些方法必须是在这个项目中自己能够用到的，并且其他同事也可能用到的方法。
 * 注意：如果这些方法不仅仅在该项目中能用到，而且可能在未来的项目中也能用到，那么请写到Utils中
 */
object BizUtils {
  //当前月的几号可能会变
  def curDateInBiz = {
    DateUtils.getCurrDate
  }

  /**
   * 根据业务，如果传入的是当前月，那么说明当前月的天数是>=15的。当前月的当前天数(15~31)
   */
  def getMonthAndDaysNumMap(monthNums: Int, formatText: String): mutable.Map[String, Int] = {
    getMonthAndDaysNumMap(getLastMonths(monthNums, formatText: String), formatText)
  }

  def getMonthAndDaysNumMap(monthsList: scala.collection.mutable.ListBuffer[String], formatText: String): mutable.Map[String, Int] = {
    val monthAndDaysNumMap = scala.collection.mutable.Map[String, Int]()
    val currentMonths = DateUtils.getStrDate(formatText)
    monthsList.foreach { month =>
      //当前月，取当前天数
      if (currentMonths == month) {
        if (curDateInBiz < 15) {
          throw new Exception("当前月的天数不能小于15号，传入的month参数可能出错了")
        }
        monthAndDaysNumMap += month -> curDateInBiz
      } else {
        monthAndDaysNumMap += month -> DateUtils.strToCalendar(month, formatText).getActualMaximum(Calendar.DATE)
      }
    }
    monthAndDaysNumMap
  }

  /**
   * 1. 扣除特殊月(即2月)
   * 2. 采集数据截止日期为15号之后(包含15号)，则包含当前月，否则从上个自然月算起。
   */
  def getInitYearAndMonth(formatText: String) = {
    val curDate = curDateInBiz
    val curMonth = DateUtils.getCurrMonth
    if (curDate < 15 && curMonth == 3) {
      //如果当前月天数小于15，且当前月是3月份，那么要回退到一月份。
      DateUtils.monthsAgo(2, formatText)
    } else if (curDate < 15 && curMonth != 3) {
      //如果当前月天数小于15，除3月份外，其他月份回退一个月。
      DateUtils.monthsAgo(1, formatText)
    } else if (curDate >= 15 && curMonth == 2) {

      //如果当前月天数大于15，且当前月是2月份，那么回退一个月。
      DateUtils.monthsAgo(1, formatText)
    } else {
      //如果当前月天数大于15，且当前月不是2月份，不需要回退。
      DateUtils.getStrDate(formatText)
    }
  }

  def getDaysNumInMonth(strDate: String, formatText: String = Constants.App.YEAR_MONTH_FORMAT): Int = {
    val paramYearAndMonth = DateUtils.cutYearMonthDay(strDate)
    val currYearAndMonth = DateUtils.getStrDate(formatText)
    if (paramYearAndMonth == currYearAndMonth) curDateInBiz
    else DateUtils.getTotalDayInMonth(strDate, formatText)
  }

  /**
   * 计算近N个月。
   * 近N个月指当前月往前N个自然月，N个月中不包含特殊月份。
   * 起始月处理：如采集到数据截止日期为15号之后(包含15号)，则起始月为当前月，否则从上个自然月算起。
   * 特殊月份处理：2月份为特殊月。本文档所有涉及到月份数的，都是扣除2月后的月份数。
   * 如近12个月实际为从近13个月数据中剔除2月份数据得到的。
   * 如部分商户的订单数据只有N个月的（N<12），则也应扣除特殊月份，即实际有效月份数为N-1个月。
   */
  def getLastMonths(num: Int, formatText: String) = {
    //放在内部可能在spark中会有问题
    def minusOneMonth(calendar: Calendar) {
      calendar.add(Calendar.MONTH, -1)
      //2月份不计算在内
      if (calendar.get(Calendar.MONTH) == 1)
        calendar.add(Calendar.MONTH, -1)
    }
    val calendar = Calendar.getInstance()
    if (curDateInBiz < 15) {
      minusOneMonth(calendar)
    }
    var list = mutable.ListBuffer[String]()
    list += DateUtils.getStrDate(calendar, formatText)
    (1 until num).foreach(a => {
      minusOneMonth(calendar)
      list += DateUtils.getStrDate(calendar, formatText)
    })
    list
  }

  def lastMonthsDayAverageSales(monthNums: Int) = {
    val lastMothsList = BizUtils.getLastMonths(monthNums, Constants.App.YEAR_MONTH_FORMAT)
    val monthAndDaysNumMap = BizUtils.getMonthAndDaysNumMap(lastMothsList, Constants.App.YEAR_MONTH_FORMAT)
    BizDao.getFakedInfoProps(SQL().select("order_date,shop_id,order_money,is_faked").where("is_faked = 'false'")) //取出的数据是净营业额(即不包含刷单的营业额)
      .map(a => (DateUtils.cutYearMonth(a(0).toString), a(1), a(2))) //(2015/5,15453,18.0)
      .filter(t => lastMothsList.contains(t._1)) //过滤出近6个月的数据
      .map(t => ((t._2, t._1), t._3.toString.toDouble)) //((15453,2015/7),15.0)
      .groupByKey() //((15453,2015/5),CompactBuffer(22.0, 15.0, 15.0, 31.0, ...))
      .map(t => (t._1._1, t._2.sum / monthAndDaysNumMap(t._1._2))) //得到了每个月的日均净营业额(15453,2360.4)
      .groupByKey() //(15453,CompactBuffer(673.0645161290323, 4914.435483870968, 1124.61904761904762, 1219.133333333333333, 4367.833333333333, 1069.6451612903227))
      .map(t => (t._1.toString, t._2.sum / monthNums)) //得到了日均净营业额：(店铺ID:15453,日均净营业额:1844.7884792626726)
  }

  /**
   * 获取店铺ID和店铺名称。
   * 之所以做的这么复杂，是因为考虑到有可能相同的店铺ID，会有不同的店铺名称。
   * 如 店铺ID为15453,店铺名称为“风云便当”。但有时候，也肯能出现店铺名称是“风云便当【满25减9】”的数据。
   * 所以以下会取最常用的店铺名称与店铺ID对应。即“风云便当【满25减9】”可能变成“风云便当”，其他不变。
   * 返回字段：(餐厅ID,餐厅名称)
   */
  def shopIDAndName(monthNum: Int): RDD[(String,String)] = {
    shopIDAndName(getLastMonths(monthNum, Constants.App.YEAR_MONTH_FORMAT))
  }

  def shopIDAndName(lastMonths: mutable.ListBuffer[String]): RDD[(String,String)] = {
    BizDao.getOrderProps(SQL().select("shop_id,shop_name,order_date")).filter(a => lastMonths.contains(DateUtils.cutYearMonth(a(2).toString))).map(a => ((a(0).toString, a(1).toString), 1)).groupByKey()
      .map(t => (t._1._1, (t._2.sum, t._1._2))).groupByKey()
      .map(t => (t._1, t._2.max)).map(t => (t._1, t._2._2))
  }
}
