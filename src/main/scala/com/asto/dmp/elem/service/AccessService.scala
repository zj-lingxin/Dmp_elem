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

  private def getDuration(list: List[String]) = {
    val sortedList = list.map(DateUtils.strToStr(_, "yyyy/M", "yyyy/MM")).sorted.reverse
    var yearAndValue = BizUtils.getInitYearAndMonth("yyyy/MM")
    var duration = 0
    var end = false
    sortedList.toStream.takeWhile(_ => !end).foreach { dateInRecords =>
      if (dateInRecords == yearAndValue) {
        duration += 1
        if (yearAndValue.contains("/03"))
          yearAndValue = backMonths(yearAndValue, 2, "yyyy/MM")
        else
          yearAndValue = backMonths(yearAndValue, 1, "yyyy/MM")
      } else if (dateInRecords < yearAndValue) {
        end = true
      }
      //如果dateInRecords > yearAndValue，什么都不做
    }
    duration
  }
}