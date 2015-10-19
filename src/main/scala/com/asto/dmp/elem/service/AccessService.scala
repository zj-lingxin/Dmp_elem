package com.asto.dmp.elem.service

import java.util.Calendar

import com.asto.dmp.elem.base.{SQL, DataSource, Constants}
import com.asto.dmp.elem.dao.BizDao
import com.asto.dmp.elem.util._

class AccessService extends DataSource {

  def run(): Unit = {

    try {
      val shopDuration = BizDao.getOrderProps(SQL().setSelect("shop_id,order_date")).
        map(a => (a(0).toString, DateUtils.cutYearMonth(a(1).toString))).distinct().groupByKey().map(t => (t._1, AccessService.getDuration(t._2.toList))).foreach(println)
    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.ACCESS_SUBJECT, t)
    }
  }
}

object AccessService {

  private def backMonths(strDate: String, m: Int, formatText: String): String = {
    val cal = DateUtils.strToCalendar(strDate, formatText)
    cal.add(Calendar.MONTH, -m)
    DateUtils.getStrDate(cal, formatText)
  }

  def getDuration(list: List[String]): Int = {
    var yearAndValue = BizUtils.getInitYearAndMonth
    val sortedList = list.sortWith((x, y) => if (DateUtils.strToDate(x, "yyyy/M").getTime > DateUtils.strToDate(y, "yyyy/M").getTime) true else false)
    var duration = 0
    var end = false
    for ( dateInRecords <- sortedList if !end) {
      val dateInRecordsLong = DateUtils.strToDate(dateInRecords, "yyyy/M").getTime
      val yearAndValueLong = DateUtils.strToDate(yearAndValue, "yyyy/M").getTime
      if (dateInRecordsLong == yearAndValueLong) {
        if (dateInRecords == yearAndValue) {
          duration += 1
          if (yearAndValue.contains("/3")) {
            yearAndValue = backMonths(yearAndValue, 2, "yyyy/M")
          } else {
            yearAndValue = backMonths(yearAndValue, 1, "yyyy/M")
          }
        }
      } else if (dateInRecordsLong < yearAndValueLong){
        end = true
      }
    }
    duration
  }
}