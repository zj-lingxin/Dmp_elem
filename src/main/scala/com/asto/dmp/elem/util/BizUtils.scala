package com.asto.dmp.elem.util

import java.util.Calendar

import com.asto.dmp.elem.base.{Contexts, Constants}
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

  def getDaysNumInMonth(strDate: String, formatText: String = "yyyy/M"): Int = {
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

  //remember delete!!
  def tempConvertData(): Unit = {
    Contexts.getSparkContext.textFile(Constants.InputPath.ORDER).map(_.split("\t")).filter(_.length == 13).map(_.toList.mkString("\t")).coalesce(1).saveAsTextFile("hdfs://appcluster/elem/input/order2")
  }

  //remember delete!!
  def testTempConvert() = {
    val rdd = Contexts.getSparkContext.textFile("hdfs://appcluster/elem/input/order").map(_.split("\t"))
    rdd.map(_.length).distinct().foreach(println)
    rdd.map(a => (a(2), a(3))).distinct().foreach(println)
  }

}
