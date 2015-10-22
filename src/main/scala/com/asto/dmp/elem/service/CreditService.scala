package com.asto.dmp.elem.service

import com.asto.dmp.elem.base._
import com.asto.dmp.elem.dao.CreditDao
import com.asto.dmp.elem.util.{Utils, FileUtils}

/**
 * 授信规则
 *
 * 营业额(就算有退款，也是算在营业额中的)
 * 基于商户近12个月的营业额，计算该商户的授信额度。
 *
 * 名称说明：
 * 月净营业额指该月营业额与该月刷单额之差
 * 月日净营业额 = 月净营业额/每个月的天数
 * 营业额加权环比增长率(SalesRateWeighting):略
 * 贷款倍率:该值由营业额加权环比增长率（SalesRateWeighting）和准入规则中定义的日均净营业额（DayAverageSales）这两个参数决定。
 *
 * 授信额度由近12个月的净营业额均值和贷款倍率来计算，上限为50万元，具体计算公式如下：
 * 授信额度=MIN[近12个月日营业额均值*（1-刷单率）*30*β，500000]
 * 注：不满12个月的营业额，“近12个月日营业额均值”为近N个月日营业额均值总和除以12。
 *
 * 输出结果：
 * 餐厅id, 餐厅名称, 营业额加权环比增长率, 日均净营业额, 贷款倍率, 近12个月日营业额均值, 刷单率, 授信额度, 是否准入
 */
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
      val lineOfCredit = last12MonthsAvgSales.leftOuterJoin(fakedRate) //(15453,(3228.913844086022,Some((风云便当,0.1))))
        .filter(t => t._2._2.isDefined)
        .map(t => (t._1, (t._2._1, t._2._2.get._1, t._2._2.get._2))) //(15453,(3228.913844086022,风云便当,0.1))
        .leftOuterJoin(beta) //(餐厅id,((近12个月日营业额均值,餐厅名称,刷单率),Some((营业额加权环比增长率,日均净营业额,贷款倍率(即β),是否准入))))
        .map(t => (t._1, t._2._1._2, t._2._2.get._1, t._2._2.get._2, t._2._2.get._3, t._2._1._1, t._2._1._3, t._2._2.get._4)) //(餐厅id,餐厅名称,营业额加权环比增长率,日均净营业额,β,近12个月日均营业额均值,刷单率)
        .map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, Math.min(t._6 * (1 - t._7) * 30 * t._5,  CreditDao.loanCeiling), t._8))

      FileUtils.deleteHdfsFiles(Constants.OutputPath.CREDIT_TEXT)
      //输出文件的字段：餐厅id, 餐厅名称, 营业额加权环比增长率, 日均净营业额, 贷款倍率, 近12个月日营业额均值, 刷单率, 授信额度, 是否准入
      lineOfCredit.map(_.productIterator.mkString(Constants.OutputPath.SEPARATOR)).coalesce(1).saveAsTextFile(Constants.OutputPath.CREDIT_TEXT)

    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.CREDIT_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("授信模型运行结束"))
    }
  }
}
