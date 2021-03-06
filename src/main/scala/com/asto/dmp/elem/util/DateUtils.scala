package com.asto.dmp.elem.util

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

object DateUtils extends scala.Serializable{
  private val cal = Calendar.getInstance()

  /**
   * 获取当前系统日期。
   * 参数formatText可以是任意的yyyy MM dd HH mm ss组合,如"yyyy-MM-dd"、"yyyy-MM/dd"。
   */
  def getStrDate(formatText: String): String = getStrDate(Calendar.getInstance(), formatText)

  /**
   * 获取当前系统日期。
   * 参数calendar可以调节日期，如调到12个月之前，并且使用formatText格式输出
   * 参数可以是任意的yyyy MM dd HH mm ss组合,如"yyyy-MM-dd"、"yyyy-MM/dd"。
   * 如果不传参数，则默认是"yyyy-MM-dd"
   */
  def getStrDate(calendar: Calendar, formatText: String = "yyyy-MM-dd") =
    new java.text.SimpleDateFormat(formatText).format(calendar.getTime)

  /**
   * 获取当前系统日期。
   * 格式是"yyyy-MM-dd HH:mm:ss"
   */
  def getStrDate: String = getStrDate("yyyy-MM-dd")

  /**
   * 倒推出m天之前的日期，并以formatText格式以字符串形式输出
   */
  def daysAgo(m: Int = 0, formatText: String = "yyyy-MM-dd"): String =
    timeAgo(m, Calendar.DATE, formatText)

  /**
   * 倒推出m月之前的日期，并以formatText格式以字符串形式输出
   */
  def monthsAgo(m: Int = 0, formatText: String = "yyyy-MM-dd"): String =
    timeAgo(m, Calendar.MONTH, formatText)

  /**
   * 倒推出m年之前的日期，并以formatText格式以字符串形式输出
   */
  def yearAgo(m: Int = 0, formatText: String = "yyyy-MM-dd"): String =
    timeAgo(m, Calendar.YEAR, formatText)

  def timeAgo(m: Int = 0, field: Int = Calendar.DATE, formatText: String = "yyyy-MM-dd"): String = {
    val cal = Calendar.getInstance()
    cal.add(field, -m)
    getStrDate(cal, formatText)
  }

  /**
   * 截取出例如：2015-04-03、2015-4-3、2015/04/03、2015/4/3这样的字符串
   */
  def cutYearMonthDay(strDate: String) = {
    val regex = """\d{4}(/|-)\d{1,2}(/|-)\d{1,2}""".r
    regex.findFirstIn(strDate).getOrElse("")
  }

  /**
   * 截取出例如：2015-04、2015-4、2015/04、2015/4这样的字符串
   */
  def cutYearMonth(strDate: String) = {
    val regex = """\d{4}(/|-)\d{1,2}""".r
    regex.findFirstIn(strDate).getOrElse("")
  }

  /**
   * 获取当前年份
   */
  def getCurrYear: Int = {
    cal.get(Calendar.YEAR)
  }

  /**
   * 获取当前月份
   */
  def getCurrMonth: Int = {
    cal.get(Calendar.MONTH) + 1
  }

  /**
   * 获取当前日
   */
  def getCurrDate: Int = {
    cal.get(Calendar.DATE)
  }

  /**
   * 将String类型的日期转化为Calendar
   */
  def strToCalendar(strDate: String, formatText: String = "yyyy-MM-dd"): Calendar = {
    val sdf= new SimpleDateFormat(formatText)
    val date = sdf.parse(strDate)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar
  }

  def strToDate(strDate: String, formatText: String = "yyyy-MM-dd"): Date = {
    val sdf= new SimpleDateFormat(formatText)
    sdf.parse(strDate)
  }

  def strToStr(strDate: String, oldFormat: String, newFormat: String) =
    dateToStr(strToDate(strDate, oldFormat),newFormat)


  def dateToStr(date: Date, formatText: String = "yyyy-MM-dd" ): String = {
    val sdf= new SimpleDateFormat(formatText)
    sdf.format(date)
  }

  def getTotalDayInMonth(strDate: String, formatText: String = "yyyy-MM-dd") =
    strToCalendar(strDate, formatText).getActualMaximum(Calendar.DAY_OF_MONTH)

  /**
   * 获取属于哪个季度
   */
  def getQuarterStartTime(strDate: String, formatText: String): String = {
    val c = strToCalendar(strDate, formatText)
    val currentMonth = c.get(Calendar.MONTH) + 1
    if (currentMonth >= 1 && currentMonth <= 3)
      c.set(Calendar.MONTH, 0)
    else if (currentMonth >= 4 && currentMonth <= 6)
      c.set(Calendar.MONTH, 3)
    else if (currentMonth >= 7 && currentMonth <= 9)
      c.set(Calendar.MONTH, 6)
    else if (currentMonth >= 10 && currentMonth <= 12)
      c.set(Calendar.MONTH, 9)
    c.set(Calendar.DATE, 1)
    new SimpleDateFormat(formatText).format(c.getTime)
  }

}
