package com.asto.dmp.elem.service

import com.asto.dmp.elem.base._
import com.asto.dmp.elem.dao.CreditDao
import com.asto.dmp.elem.util.{Utils, FileUtils}

class CreditService extends DataSource with scala.Serializable {

  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行授信模型"))
      //贷款倍率
      val beta = CreditDao.getBetaInfo

      //计算近12个月日营业额均值
      val last12MonthsAvgSales = CreditDao.getLast12MonthsAvgSales

      //刷单率
      val fakedRate =  CreditDao.getFakedRate

      //计算授信额度：授信额度=MIN[近12个月日营业额均值*（1-刷单率）*30*β，500000]
      //输出：餐厅id、餐厅名称	、营业额加权环比增长率、日均净营业额、 贷款倍率、	近12个月日营业额均值	、刷单率、授信额度
      val lineOfCredit = last12MonthsAvgSales.leftOuterJoin(fakedRate) //(15453,(3228.913844086022,Some((风云便当,0.1))))
        .filter(t => t._2._2.isDefined)
        .map(t => (t._1, (t._2._1, t._2._2.get._1, t._2._2.get._2))) //(15453,(3228.913844086022,风云便当,0.1))
        .leftOuterJoin(beta) //(餐厅id,((近12个月日营业额均值,餐厅名称,刷单率),Some((营业额加权环比增长率,日均净营业额,贷款倍率(即β)))))
        .map(t => (t._1, t._2._1._2, t._2._2.get._1, t._2._2.get._2, t._2._2.get._3, t._2._1._1, t._2._1._3)) //(餐厅id,餐厅名称,营业额加权环比增长率,日均净营业额,β,近12个月日均营业额均值,刷单率)
        .map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, Math.min(t._6 * (1 - t._7) * 30 * t._5,  CreditDao.loanCeiling)))

      FileUtils.deleteHdfsFiles(Constants.OutputPath.CREDIT_TEXT)
      //保存到输出文件的字段："餐厅id", "餐厅名称", "营业额加权环比增长率", "日均净营业额", "贷款倍率", "近12个月日营业额均值", "刷单率", "授信额度"
      lineOfCredit.map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile(Constants.OutputPath.CREDIT_TEXT)

    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.CREDIT_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("授信模型运行结束"))
    }
  }
}
