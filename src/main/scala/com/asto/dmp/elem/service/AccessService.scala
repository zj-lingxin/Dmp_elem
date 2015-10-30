package com.asto.dmp.elem.service

import com.asto.dmp.elem.base.Constants
import com.asto.dmp.elem.dao.AccessDao
import com.asto.dmp.elem.util.mail.MailAgent
import com.asto.dmp.elem.util.{Utils, FileUtils}

/**
 * 准入规则
 *
 * 基于商户近6月的营业额，判定该商户是否有贷款准入资格。
 * 该规则由5个指标组成，只有所有指标均满足其对应的贷前阈值，才能获得贷款准入资格。
 *
 * 名称说明：
 * 日营业额指该日所有订单额之和
 * 日刷单金额指该日所有刷单的订单总金额
 * 日净营业额指该日营业额与该日刷单金额之差
 *
 * 序号  指标名称	                                      指标释义	                                            贷前阈值
 * 1    平台连续经营时间(ShopDuration)  	                在饿了么平台连续有交易额的自然月月份数。                  ShopDuration ≥ 6(贷前阈值)
 *                                                    注：1. 扣除特殊月 2.采集数据截止日期为15号之后(包含15号)
 *                                                    则包含当前月，否则从上个自然月算起。
 * 2    日均净营业额(DayAverageSales)	                  近6个月日均净营业额之和 / 6	                            DayAverageSales≥1000元
 * 3    基于3个月的营业额增长率 (SaleRateInThreeMonths)  近3个月日净营业额均值/近6个月日净营业额均值               SaleRateInThreeMonths≥1
 * 4    基于1个月的营业额增长率（SaleRateInOneMonths）	  近1个月日净营业额均值/近6个月日净营业额均值               SaleRateInOneMonths≥1
 * 5    刷单率（FakedSalesRate）	                      近6个月总刷单金额/近6个月总营业额(不折算)	                FakedSalesRate≤20%
 *
 * 输出结果：
 * 餐厅ID,餐厅名称,平台连续经营时间,日均净营业额,基于3个月的营业额增长率,基于1个月的营业额增长率,刷单率,是否准入
 */
class AccessService extends Service {

  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行准入模型"))

      val fakedRate = AccessDao.getFakedSalesRate
      val shopDuration = AccessDao.getShopDuration
      val dayAverageSales = AccessDao.getDayAverageSales
      val saleRateInOneMonths = AccessDao.getSaleRateInOneMonths
      val saleRateInThreeMonths = AccessDao.getSaleRateInThreeMonths

      //输出文件的字段：餐厅ID,餐厅名称,平台连续经营时间,日均净营业额,基于3个月的营业额增长率,基于1个月的营业额增长率,刷单率,是否准入
      val resultRDD = fakedRate.leftOuterJoin(shopDuration)
        .leftOuterJoin(dayAverageSales)
        .leftOuterJoin(saleRateInOneMonths)
        .leftOuterJoin(saleRateInThreeMonths) //(17644,(((((美食美客,0.0850489185650941),Some(14)),Some(1372.866858678956)),Some(0.013666223088784037)),Some(0.2516462000131865)))
        .map(t => (t._1, t._2._1._1._1._1._1, t._2._1._1._1._2.getOrElse(0), t._2._1._1._2.getOrElse(0), t._2._2.getOrElse(0), t._2._1._2.getOrElse(0), t._2._1._1._1._1._2))
        .map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._3 >= 6 && t._4.toString.toDouble >= 1000 && t._5.toString.toDouble >= 1 && t._6.toString.toDouble >= 1 && t._7 <= 0.2))
      FileUtils.saveAsTextFile(resultRDD, Constants.OutputPath.ACCESS_TEXT)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.ACCESS_SUBJECT).sendMessage()
        logError(Constants.Mail.ACCESS_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("准入模型运行结束"))
    }
  }
}
