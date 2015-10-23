package com.asto.dmp.elem.service

import com.asto.dmp.elem.base.{Constants, DataSource}
import com.asto.dmp.elem.dao.LoanWarningDao
import com.asto.dmp.elem.util.mail.MailAgent
import com.asto.dmp.elem.util.{FileUtils, BizUtils, Utils}

/**
 * 贷后预警规则
 *
 * 基于商户近6月的营业额，判定该商户是否有贷后失信风险，两个条件中任一条件满足即发出预警。
 * 名称说明：
 * 月营业额指该月所有订单额之和
 * 月刷单金额指该月所有按反欺诈规则判定为刷单的订单总金额
 * 月净营业额指该月营业额与该月刷单金额之差
 *
 * 指标编号 指标名称	                                      指标释义	                                 贷后预警条件
 * 指标1   基于3个月的营业额增长率（SaleRateInThreeMonths）	近3个月日净营业额均值/近6个月日净营业额均值	   SaleRateInThreeMonths<0.7
 * 指标2   基于1个月的营业额增长率（SaleRateInOneMonths）	  近1个月日净营业额均值/近6个月日净营业额均值	   SaleRateInOneMonths<0.7
 *
 * 输出文件的字段：餐厅ID, 餐厅名称,预警指标1,	预警指标2	,是否预警
 */
class LoanWarningService extends DataSource with scala.Serializable {
  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行贷后模型"))
      val saleRateInOneMonths = LoanWarningDao.getSaleRateInOneMonths
      val saleRateInThreeMonths = LoanWarningDao.getSaleRateInThreeMonths

      //输出文件的字段：餐厅ID, 餐厅名称,预警指标1,	预警指标2	,是否预警
      val resultRDD = saleRateInThreeMonths.leftOuterJoin(saleRateInOneMonths)
        .leftOuterJoin(BizUtils.shopIDAndName(6)) //(17644,((0.2516462000131865,Some(0.013666223088784037)),Some(美食美客)))
        .map(t => (t._1, t._2._2.getOrElse(0), t._2._1._1, t._2._1._2.getOrElse(0), t._2._1._1 < 0.7 || t._2._1._2.get < 0.7))
      FileUtils.saveAsTextFile(resultRDD, Constants.OutputPath.LOAN_WARNING_TEXT)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.LOAN_WARNING_SUBJECT).sendMessage()
        logError(Constants.Mail.LOAN_WARNING_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("贷后模型运行结束"))
    }
  }
}

