package com.asto.dmp.elem.service

import com.asto.dmp.elem.base.{Constants, DataSource}
import com.asto.dmp.elem.dao.LoanWarningDao
import com.asto.dmp.elem.util.{FileUtils, BizUtils, Utils}

class LoanWarningService extends DataSource with scala.Serializable {
  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行贷后模型"))
      val saleRateInOneMonths = LoanWarningDao.getSaleRateInOneMonths
      val saleRateInThreeMonths = LoanWarningDao.getSaleRateInThreeMonths

      FileUtils.deleteHdfsFiles(Constants.OutputPath.LOAN_WARNING_TEXT)
      //保存到输出文件的字段："餐厅id", "餐厅名称", "营业额加权环比增长率", "日均净营业额", "贷款倍率", "近12个月日营业额均值", "刷单率", "授信额度"
      saleRateInThreeMonths.leftOuterJoin(saleRateInOneMonths)
        .leftOuterJoin(BizUtils.shopIDAndName(6))//(17644,((0.2516462000131865,Some(0.013666223088784037)),Some(美食美客)))
        .map(t => (t._1, t._2._2.get, t._2._1._1,t._2._1._2.get, t._2._1._1 < 0.7 || t._2._1._2.get < 0.7 ))
        .map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile(Constants.OutputPath.LOAN_WARNING_TEXT)
    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.LOAN_WARNING_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("贷后模型运行结束"))
    }
  }
}

