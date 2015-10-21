package com.asto.dmp.elem.service

import com.asto.dmp.elem.base.{DataSource, Constants}
import com.asto.dmp.elem.dao.AccessDao
import com.asto.dmp.elem.util.FileUtils

class AccessService extends DataSource {

  def run(): Unit = {
    try {
      val fakedRate = AccessDao.getFakedSalesRate
      val shopDuration = AccessDao.getShopDuration
      val dayAverageSales = AccessDao.getDayAverageSales
      val saleRateInOneMonths = AccessDao.getSaleRateInOneMonths
      val saleRateInThreeMonths = AccessDao.getSaleRateInThreeMonths
      FileUtils.deleteHdfsFiles(Constants.OutputPath.ACCESS_TEXT)
      fakedRate.leftOuterJoin(shopDuration)
        .leftOuterJoin(dayAverageSales)
        .leftOuterJoin(saleRateInOneMonths)
        .leftOuterJoin(saleRateInThreeMonths) //(17644,(((((美食美客,0.0850489185650941),Some(14)),Some(1372.866858678956)),Some(0.013666223088784037)),Some(0.2516462000131865)))
        .map(t => (t._1, t._2._1._1._1._1._1, t._2._1._1._1._2.get, t._2._1._1._2.get, t._2._2.get, t._2._1._2.get, t._2._1._1._1._1._2)) //(餐厅ID:17644,餐厅名称:美食美客,平台连续经营时间:14,日均净营业额:1372.866858678956,基于3个月的营业额增长率:1.2516462000131865,基于1个月的营业额增长率:1.013666223088784037,刷单率：0.0850489185650941)
        .map(t => (t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._3 >= 6 && t._4 >= 1000 && t._5 >= 1 && t._6 >= 1 && t._7 <= 0.2))
        .map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile(Constants.OutputPath.ACCESS_TEXT)
    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.ACCESS_SUBJECT, t)
    }
  }
}
