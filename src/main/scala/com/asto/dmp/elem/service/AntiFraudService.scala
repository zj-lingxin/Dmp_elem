package com.asto.dmp.elem.service

import com.asto.dmp.elem.base._
import com.asto.dmp.elem.dao.AntiFraudDao
import com.asto.dmp.elem.util.{DateUtils, BizUtils, FileUtils}

class AntiFraudService extends DataSource with Serializable {

  def run(): Unit = {

    try {
      //输出1：订单ID, 订单日期, 餐厅ID ,餐厅名称 ,下单客户ID	,下单时间	,订单额 ,刷单指标值1	,刷单指标值2,	刷单指标值3,	刷单指标值4,	刷单指标值5,	是否刷单
      val FQZ1 = AntiFraudDao.getFQZ1Info.map(t => (t._2.toString, (t._3, t._4))) // (订单号,(FQZ1,true/false))
      val FQZ2 = AntiFraudDao.getFQZ2Info.map(t => (t._2.toString, (t._3, t._4))) // (订单号,(FQZ2,true/false))*/
      val FQZ3 = AntiFraudDao.getFQZ3Info.map(t => (t._1.toString, (t._2, t._4, t._5, t._6))) // (订单号,(订单日期,客户ID,FQZ3,复购率(FQZ3)是否大于2：true/false))
      val FQZ4 = AntiFraudDao.getFQZ4Info.map(t => (t._2.toString, (t._3, t._4))) // (订单号,(下单距离(FQZ4),是否下单距离异常))
      val FQZ5 = AntiFraudDao.getFQZ5Info.map(t => (t._1.toString, (t._2, t._4, t._5, t._6, t._7, t._8))) //(订单号,(订单额,订单日期,店铺ID,店铺名称,FQZ5,下单时间,是否异常))

      val fakedInfo = FQZ1.leftOuterJoin(FQZ2).filter(t => t._2._2.isDefined) //(12663421529953153,((0.7950705636866545,false),Some((0.8342663644556104,false))))
        .map(t => (t._1, (t._2._1._1, t._2._2.get._1, t._2._1._2, t._2._2.get._2))) //(订单号,(FQZ1,FQZ2,FQZ1_boolean,FQZ2_boolean))
        .leftOuterJoin(FQZ3).filter(t => t._2._2.isDefined) //(12870016044179153,((1.0397076602056252,0.9568194322743668,false,false),Some((2015/5/22,3417575,FQZ3,false))))
        .map(t => (t._1, (t._2._2.get._1, t._2._2.get._2, t._2._1._1, t._2._1._2, t._2._2.get._3, t._2._1._4, t._2._1._4, t._2._2.get._4))) //(订单号,(订单日期,客户ID,FQZ1,FQZ2,FQZ3,FQZ1_boolean,FQZ2_boolean,FQZ3_boolean))
        .leftOuterJoin(FQZ4).filter(t => t._2._2.isDefined) //(12471994367863353,((2015/6/24,6631826,0.5504334671676839,0.5278958114894321,2,false,false,false),Some((0.7390250704059941,false))))
        .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._2.get._1, t._2._1._6, t._2._1._7, t._2._1._8, t._2._2.get._2))) //(订单号,(订单日期,客户ID,FQZ1,FQZ2,FQZ3,FQZ4,FQZ1_boolean,FQZ2_boolean,FQZ3_boolean,FQZ4_boolean))
        .leftOuterJoin(FQZ5).filter(t => t._2._2.isDefined) //(12570823842359853,((2015/5/23,1520316,0.9173891119461399,0.8688698670050893,2,0.9101241366519388,false,false,false,false),Some((15.0,15453,风云便当,2015/5/23 17:52,0.8748100303951367,false))))
        .map(t => (t._1, t._2._1._1, t._2._2.get._2, t._2._2.get._3.toString, t._2._1._2, t._2._2.get._4, t._2._2.get._1, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._2.get._5, t._2._1._7.toString.toBoolean || t._2._1._8.toString.toBoolean || t._2._1._9.toString.toBoolean || t._2._1._10.toString.toBoolean || t._2._2.get._6.toString.toBoolean)) //(12268945514238653,2015/4/20,15453,风云便当,2435049,2015/4/20 17:31,15.0,0.9173891119461399,0.8543417366946778,1,1.0280087030410778,0.8772853450751412,false,false,false,false,false)

      fakedInfo.persist()

      FileUtils.deleteHdfsFiles(Constants.OutputPath.ANTI_FRAUD_FAKED_INFO_TEXT)
      fakedInfo.map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile(Constants.OutputPath.ANTI_FRAUD_FAKED_INFO_TEXT)

      //输出2：餐厅ID,餐厅名称,近6个月刷单金额,近6个月总营业额,刷单率
      val lastSixMonthsList = BizUtils.getLastMonths(6, "yyyy/M")
      val result2NeedsData = fakedInfo.map(t => (DateUtils.cutYearMonth(t._2), t._3, t._4, t._7, t._13))
        .filter(t => lastSixMonthsList.contains(t._1.toString)).persist() //(2015/6,15453,风云便当,15.0,false)
      //近6个月总营业额
      val lastSixMonthsTotalSales = result2NeedsData.map(t => (t._2, t._4))
        .groupByKey().map(t => (t._1, t._2.sum)) //(15453,390662.0)
      //近6个月刷单金额
      val lastSixMonthsFakedSales = result2NeedsData.filter(_._5.toString.toBoolean)
        .map(t => (t._2, t._4)).groupByKey().map(t => (t._1, t._2.sum))

      //获取店铺ID和店铺名称。之所以做的这么复杂，是因为考虑到有可能相同的店铺ID，会有不同的店铺名称。
      //如 店铺ID为15453,店铺名称为“风云便当”。但有时候，也肯能出现店铺名称是“风云便当【满25减9】”的数据。
      //所以以下会取最常用的店铺名称与店铺ID对应。即“风云便当【满25减9】”可能变成“风云便当”，其他不变。
      val shopIDAndName = result2NeedsData.map(t => ((t._2, t._3), 1)).groupByKey()
        .map(t => (t._1._1, (t._2.sum, t._1._2))).groupByKey()
        .map(t => (t._1, t._2.max)).map(t => (t._1, t._2._2)) //(餐厅ID,餐厅名称)
      val lastSixMonthsFakedRate = shopIDAndName.leftOuterJoin(lastSixMonthsFakedSales) //(15453,(风云便当,Some(53255.5)))
        .leftOuterJoin(lastSixMonthsTotalSales) //(15453,((风云便当,Some(53255.5)),Some(390662.0)))
        .filter(t => t._2._2.get.toString.toDouble > 0)
        .map(t => (t._1, t._2._1._1, t._2._1._2.getOrElse(0).toString, t._2._2.get.toString, t._2._1._2.getOrElse(0D).toString.toDouble / t._2._2.get.toString.toDouble)).persist()
      FileUtils.deleteHdfsFiles(Constants.OutputPath.ANTI_FRAUD_FAKED_RATE_TEXT)
      lastSixMonthsFakedRate.map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile(Constants.OutputPath.ANTI_FRAUD_FAKED_RATE_TEXT)
      fakedInfo.unpersist()

    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.ANTI_FRAUD_SUBJECT, t)
    }
  }
}
