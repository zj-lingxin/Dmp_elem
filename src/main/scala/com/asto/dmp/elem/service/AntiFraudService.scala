package com.asto.dmp.elem.service

import java.text.SimpleDateFormat
import java.util.Calendar

import com.asto.dmp.elem.base._
import com.asto.dmp.elem.dao.{AntiFraudDao, BizDao}
import com.asto.dmp.elem.util.{DateUtils, BizUtils}

class AntiFraudService extends DataSource with Serializable {


/*  def strDayToCalendar(strDay: String) = {
    val calendar = Calendar.getInstance()
    val date = sdf.parse(strDay)
    calendar.setTime(date)
    calendar
  }*/

  def run(): Unit = {

    try {
      //val FQZ1 = AntiFraudDao.getFQZ1Info
      //val FQZ2 = AntiFraudDao.getFQZ2Info
      //val FQZ3 = AntiFraudDao.getFQZ3Info
      //val FQZ4 = AntiFraudDao.getFQZ4Info
      val FQZ5 = AntiFraudDao.getFQZ5Info
    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.ANTI_FRAUD_SUBJECT, t)
    }
  }
}

object AntiFraudService {

}