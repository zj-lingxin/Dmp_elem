package com.asto.dmp.elem.util

import java.util.Calendar

object DateUtils {
  private val df = Calendar.getInstance()

  /**
   * 获取当前系统日期。
   * 参数formatText可以是任意的yyyy MM dd HH mm ss组合,如"yyyy-MM-dd"、"yyyy-MM/dd"。
   */
  def getStrDate(formatText: String): String = getStrDate(Calendar.getInstance(), formatText)

  /**
   * 获取当前系统日期。
   * 参数calendar可以调节日期，如调到12个月之前，并且使用formatText格式输出
   * 参数可以是任意的yyyy MM dd HH mm ss组合,如"yyyy-MM-dd"、"yyyy-MM/dd"。
   * 如果不传参数，则默认是"yyyy-MM-dd HH:mm:ss"
   */
  def getStrDate(calendar: Calendar, formatText: String = "yyyy-MM-dd HH:mm:ss") =
    new java.text.SimpleDateFormat(formatText).format(calendar.getTime)

  /**
   * 获取当前系统日期。
   * 格式是"yyyy-MM-dd HH:mm:ss"
   */
  def getStrDate: String = getStrDate("yyyy-MM-dd HH:mm:ss")

  /**
   * 倒推出m天之前的日期，并以formatText格式以字符串形式输出
   */
  def daysAgo(m: Int = 0, formatText: String = "yyyyMMdd"): String =
    timeAgo(m, Calendar.DATE, formatText)

  /**
   * 倒推出m月之前的日期，并以formatText格式以字符串形式输出
   */
  def monthsAgo(m: Int = 0, formatText: String = "yyyyMMdd"): String =
    timeAgo(m, Calendar.MONTH, formatText)

  /**
   * 倒推出m年之前的日期，并以formatText格式以字符串形式输出
   */
  def yearAgo(m: Int = 0, formatText: String = "yyyyMMdd"): String =
    timeAgo(m, Calendar.YEAR, formatText)

  def timeAgo(m: Int = 0, field: Int = Calendar.DATE, formatText: String = "yyyyMMdd"): String = {
    val cal = Calendar.getInstance()
    cal.add(field, -m)
    getStrDate(cal, formatText)
  }

  /**
   * 获取当前年份
   */
  def getCurrYear: Int = {
    df.get(Calendar.YEAR)
  }

  /**
   * 获取当前月份
   */
  def getCurrMonth: Int = {
    df.get(Calendar.MONTH) + 1
  }

  /**
   * 获取当前日
   */
  def getCurrDate: Int = {
    df.get(Calendar.DATE)
  }
}
