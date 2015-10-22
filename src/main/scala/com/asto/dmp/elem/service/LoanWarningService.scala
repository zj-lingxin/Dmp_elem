package com.asto.dmp.elem.service

import com.asto.dmp.elem.base.{Constants, DataSource}
import com.asto.dmp.elem.util.Utils

class LoanWarningService extends DataSource with scala.Serializable {
  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行贷后模型"))
    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.LOAN_WARNING_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("贷后模型运行结束"))
    }
  }
}

