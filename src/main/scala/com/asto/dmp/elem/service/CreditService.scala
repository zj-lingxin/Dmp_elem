package com.asto.dmp.elem.service

import com.asto.dmp.elem.base._
import com.asto.dmp.elem.dao.{BizDao}
import com.asto.dmp.elem.util.{Utils, DateUtils, FileUtils}

class CreditService extends DataSource with scala.Serializable {


  def run(): Unit = {
    try {

      FileUtils.deleteHdfsFile(Constants.OutputPath.CREDIT_TEXT)
      FileUtils.deleteHdfsFile(Constants.OutputPath.CREDIT_PARQUET)

      //刷单
      val cilckFramInfo =  sc.parallelize(Seq(
        ("	2015/6/13	","	12574664669042353	","	15453	","	风云便当	","	15.0 	","	0	"),
        ("	2015/6/13	","	12574865121584353	","	15453	","	风云便当	","	24.0 	","	1	"),
        ("	2015/6/13	","	12474664389438153	","	15453	","	风云便当	","	16.0 	","	1	"),
        ("	2015/6/13	","	12574564579168353	","	15453	","	风云便当	","	15.0 	","	0	"),
        ("	2015/6/13	","	12174964992731653	","	15453	","	风云便当	","	16.0 	","	1	"),
        ("	2015/6/13	","	12174266241880553	","	15453	","	风云便当	","	15.0 	","	1	"),
        ("	2015/6/13	","	12474064477526253	","	15453	","	风云便当	","	16.0 	","	0	"),
        ("	2015/7/13	","	12674266136499453	","	15453	","	风云便当	","	16.5 	","	1	"),
        ("	2015/7/13	","	12474664389438153	","	15453	","	风云便当	","	16.0 	","	1	"),
        ("	2015/7/13	","	12574564579168353	","	15453	","	风云便当	","	15.0 	","	0	"),
        ("	2015/7/13	","	12174964992731653	","	15453	","	风云便当	","	16.0 	","	1	"),
        ("	2015/7/13	","	12174266241880553	","	15453	","	风云便当	","	15.0 	","	1	"),
        ("	2015/7/13	","	12474064477526253	","	15453	","	风云便当	","	16.0 	","	0	"),
        ("	2015/7/13	","	12674266136499453	","	15453	","	风云便当	","	16.5 	","	1	"),
        ("	2015/8/13	","	12174866611231153	","	15453	","	风云便当	","	16.0 	","	1	"),
        ("	2015/8/13	","	12274666824734653	","	15453	","	风云便当	","	15.0 	","	0	"),
        ("	2015/8/13	","	12474464682199253	","	15453	","	风云便当	","	15.0 	","	1	"),
        ("	2015/8/13	","	12174164356238453	","	15453	","	风云便当	","	15.0 	","	0	"),
        ("	2015/8/13	","	12774265785901053	","	15453	","	风云便当	","	20.0 	","	1	"),
        ("	2015/8/13	","	12574664669042353	","	15453	","	风云便当	","	15.0 	","	0	"),
        ("	2015/8/13	","	12574865121584353	","	15453	","	风云便当	","	24.0 	","	1	"),
        ("	2015/8/13	","	12474664389438153	","	15453	","	风云便当	","	16.0 	","	1	"),
        ("	2015/8/13	","	12574564579168353	","	15453	","	风云便当	","	15.0 	","	0	"),
        ("	2015/8/13	","	12174964992731653	","	15453	","	风云便当	","	16.0 	","	1	"),
        ("	2015/8/13	","	12174266241880553	","	15453	","	风云便当	","	15.0 	","	1	"),
        ("	2015/8/13	","	12474064477526253	","	15453	","	风云便当	","	16.0 	","	0	"),
        ("	2015/8/13	","	12674266136499453	","	15453	","	风云便当	","	16.5 	","	1	"),
        ("	2015/8/13	","	12974664290772253	","	15453	","	风云便当	","	15.0 	","	0	"),
        ("	2015/7/13	","	12474064477526253	","	23112	","	吉祥馄饨	","	16.0 	","	0	"),
        ("	2015/7/13	","	12674266136499453	","	23112	","	吉祥馄饨	","	16.5 	","	1	"),
        ("	2015/8/13	","	12174866611231153	","	23112	","	吉祥馄饨	","	16.0 	","	1	"),
        ("	2015/8/13	","	12274666824734653	","	23112	","	吉祥馄饨	","	15.0 	","	0	"),
        ("	2015/8/13	","	12474464682199253	","	23112	","	吉祥馄饨	","	15.0 	","	1	"),
        ("	2015/8/13	","	12174164356238453	","	23112	","	吉祥馄饨	","	15.0 	","	0	"),
        ("	2015/8/13	","	12774265785901053	","	23112	","	吉祥馄饨	","	20.0 	","	1	"),
        ("	2015/8/13	","	12574664669042353	","	23112	","	吉祥馄饨	","	15.0 	","	0	"),
        ("	2015/8/13	","	12574865121584353	","	23112	","	吉祥馄饨	","	24.0 	","	1	"),
        ("	2015/8/13	","	12474664389438153	","	23112	","	吉祥馄饨	","	16.0 	","	1	"),
        ("	2015/8/13	","	12574564579168353	","	23112	","	吉祥馄饨	","	15.0 	","	0	"),
        ("	2015/8/13	","	12174964992731653	","	23112	","	吉祥馄饨	","	16.0 	","	1	"),
        ("	2015/8/13	","	12174266241880553	","	23112	","	吉祥馄饨	","	15.0 	","	1	"),
        ("	2015/8/13	","	12474064477526253	","	23112	","	吉祥馄饨	","	16.0 	","	0	"),
        ("	2015/8/13	","	12674266136499453	","	23112	","	吉祥馄饨	","	16.5 	","	1	"),
        ("	2015/8/13	","	12974664290772253	","	23112	","	吉祥馄饨	","	15.0 	","	0	")
      )).map(t => Utils.trimTuple(t)).map(t=>(t.productElement(1),t.productElement(3))).foreach(println)
      
      /*
            val sql = SQL("order_id, shop_id, shop_name, custom_id, custom_name", "order_id = 12254695004719553 ")
            BizDao.getOrderProps(sql).map(t=>(t(0),t(1))).take(10).foreach(println)*/

          /*  val sql2 = SQL("order_id, shop_id, shop_name, custom_id, custom_name")
            BizDao.getOrderProps(sql2).take(10).foreach(println)

            val sql3 = SQL()
            BizDao.getOrderProps(sql3).take(10).foreach(println)

            BizDao.getOrderProps().take(10).foreach(println)
*/
      //val sc.parallelize()

      /**
      CreditTable.registerTradeView()
      val totalSales = creditDao.totalSales

      CreditTable.registerBeta()
      val betaInfo = creditDao.betaInfo

      CreditTable.registerShop()
      val shopInfo = creditDao.shopInfo

      CreditTable.registerRefundInfo()
      val refundRate = creditDao.refundRate

      //val shuadanRate = creditDao.shuadanRate
      val shuadanRate = sc.parallelize(Array(
        ("2c01b6044e0f48348251fca53bb714fa", "0.12"),
        ("326c29ae272c11e5881b16ae6eea2308", "0.03"),
        ("3accb02f88f7499381c70885affc7a66", "0.23"),
        ("5b2789a7748f4319889683e4a8929da0", "0.133"),
        ("f0255ffe3da5434194badb1783ffe558", "0.0"),
        ("fcbd34e9e66640f0be797435e6bd00ab", "0.034")
      )).map(a => (a._1.toString, a._2.toDouble))

      // 大型活动暂时不考虑。
      // 这边暂时假设大型活动获取到的数据类似 Array[(Long, Double)]，定义bigEventSalesArray = 大型活动产生的交易额。
      // 数组中二元组的第一个元素表示shop_id,第二个元素表示大型活动的销售额
      val bigEventSales = sc.parallelize(Array[(String, Double)]())
      // 定义 minusBigEventSales = (前12个月销售总额-大型活动产生的交易额)
      // totalSales外连接bigEventSales。得到类似(shop_id,(销售总额，大型活动销售额))这样的数据，但是如果该shop_id没有“大型活动销售额”那么得到的数据是这样的(shop_id,(销售总额，None))
      // =>  将(shop_id,(销售总额，None))改成(shop_id,(销售总额，0))
      // =>  得到(shop_id,(销售总额-大型活动销售额,大型活动销售额))这样的数据
      val minusBigEventSales = totalSales.leftOuterJoin(bigEventSales).map(t => Pair[String, (Double, Double)](t._1, (t._2._1, t._2._2.getOrElse(0)))).map(t => (t._1, (t._2._1 - t._2._2, t._2._2)))

      // 定义 minusClickFarmingSales = [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)]
      // (shop_id,(销售总额-大型活动销售额,大型活动销售额))
      // =>  (shop_id,((销售总额-大型活动销售额,大型活动销售额),  刷单比率)
      // 例如 (   7000,((    1.850615838000001E7,       10000.0),Some(0.12))
      // =>  (shop_id,((销售总额-大型活动销售额,大型活动销售额),(1-刷单比率,刷单比率))   注意：刷单比率≤10%，不做虚假交易比率和虚假金额扣减
      // 例如 (    7000,((    1.850615838000001E7,       10000.0),(      0.88,    0.12))     即：刷单比率≤10%时，1-刷单比率 = 1.0
      // =>  (shop_id,([(销售总额-大型活动销售额)*(1-刷单比率)],大型活动销售额,刷单比率))
      // 例如 (   7000,(                    1.6285419374400008E7,       10000.0,    0.12))
      val minusClickFarmingSales = minusBigEventSales.leftOuterJoin(shuadanRate).map { t =>
        if (t._2._2.isEmpty || t._2._2.get <= 0.1)
          Pair[String, ((Double, Double), (Double, Double))](t._1, (t._2._1, (1, t._2._2.getOrElse(0))))
        else
          Pair[String, ((Double, Double), (Double, Double))](t._1, (t._2._1, (1 - t._2._2.get, t._2._2.get)))
      }.map(t => (t._1, (t._2._1._1 * t._2._2._1, t._2._1._2, t._2._2._2)))

      // (shop_id,([(销售总额-大型活动销售额)*(1-刷单比率)],大型活动销售额,刷单比率))   //minusClickFarmingSales
      // 例如(   7000,(                    1.6285419374400008E7,       10000.0,    0.12))
      // => (shop_id,(([(销售总额-大型活动销售额)*(1-刷单比率)],大型活动销售额,刷单比率),            Some(退款率))
      // 例如(   7000,((                    1.6285419374400008E7,       10000.0,    0.12),Some(0.4632258064516129))
      // => (shop_id,(         resultSales,大型活动销售额,刷单比率,            退款率)  //定义resultSales = [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]
      // 例如(   7000,(1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129)  //注意退款率的单位是 百分比
      val salesInfo = minusClickFarmingSales.leftOuterJoin(refundRate).
        map { t => (t._1, (t._2._1._1 * (1 - t._2._2.getOrElse(0).toString.toDouble * 0.01), t._2._1._2, t._2._1._3, t._2._2.getOrElse(0).toString.toDouble)) }.
        map(t => if (t._2._1 < 0) Pair[String, (Double, Double, Double, Double)](t._1, (0D, t._2._2, t._2._3, t._2._4)) else t)

      // score = 评分卡中的得分，暂时这个得分还不能获取，下面为假数据 元组的第一个元素是shop_id,第二个元素是得分score
      // val score = sc.parallelize(Array[(Long, Int)]((7000, 551),(9631, 551), (7231, 513), (7431, 576), (6431, 499), (8431, 601), (5031, 532), (9431, 544)))

      //val score = sc.textFile(CommonDefinition.PATH_SCORE_RESULT_TEXT).map(_.split(",")).map(t => (t(0), t(1).toDouble + 500))
      val score = sc.parallelize(Array(
        ("2c01b6044e0f48348251fca53bb714fa", "546"),
        ("326c29ae272c11e5881b16ae6eea2308", "513"),
        ("3accb02f88f7499381c70885affc7a66", "655"),
        ("5b2789a7748f4319889683e4a8929da0", "533"),
        ("f0255ffe3da5434194badb1783ffe558", "576"),
        ("fcbd34e9e66640f0be797435e6bd00ab", "554")
      )).map(a => (a._1, a._2.toDouble))

      // (shop_id,(         resultSales,大型活动销售额,刷单比率,            退款率)  //定义resultSales = [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]
      // 例如(   7000,(1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129)  //注意退款率的单位是 百分比
      // =>  (shop_id,((         resultSales,大型活动销售额,刷单比率,            退款率),Some(industry_type))
      // 例如(   7000,((1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),         Some(其他))
      // =>  (shop_id,((          resultSales,大型活动销售额,刷单比率,            退款率),Some(industry_type)),Some(score))
      // 例如(7000   ,(((1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),        Some(其他)), Some(551))) //注意，如果找不到得分，那么score是None
      // =>  (industry_type,(shop_id，(          resultSales,大型活动销售额,刷单比率,            退款率),score)
      // 例如(其他,         (7000,    ( 1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),551.0))
      // =>  (industry_type,((shop_id,(         resultSales,大型活动销售额,刷单比率,            退款率),score),Some((sales_upper_limit,sales_lower_limit,score_upper_limit,score_lower_limit,beta_score))))
      // 例如(其他         ,((7000   ,(1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),551.0),Some((              300,                0,              510,                 0,      0.06))))，但是这边相同的shop_id的数据是多个的，只有一个是符合要求的
      // =>  (industry_type,((shop_id,(         resultSales,大型活动销售额,刷单比率,            退款率),score),Some((sales_upper_limit,sales_lower_limit,score_upper_limit,score_lower_limit,beta_score))))   //过滤出符合条件的那条数据
      // 例如(其他         ,((7000   ,(1.6209981109168917E7,       10000.0,    0.12,0.4632258064516129),551.0),Some((             2000,              800,              559,                551,     0.16))))
      // =>  (shop_id,         resultSales,  β, industry_type, score,大型活动销售额,刷单比率,            退款率)
      // 例如(   7000,1.6209981109168917E7,0.16,          其他, 551.0,       10000.0,    0.12,0.4632258064516129)
      // =>	 (shop_id,         resultSales,  β, industry_type,score,sales*β/12*授信月份数,行业风险限额,大型活动销售额,刷单比率,            退款率)
      // 例如(   7000,1.6209981109168917E7,0.16,          其他,551.0,    1296798.4887335133,    250000.0,       10000.0,    0.12,0.4632258064516129)
      //  =>  (shop_id, (         resultSales,  β, industry_type,score,sales*β/12*授信月份数,行业风险限额,产品限额,MIN{resultSales*β/12*授信月份数,行业风险限额,产品限额}，大型活动销售额,刷单比率,             退款率))
      // 例如(   7000, (1.6209981109168917E7,0.16,          其他,551.0,    1296798.4887335133,    250000.0,  500000,                                               250000.0,        10000.0,    0.12, 0.4632258064516129))
      val tmpResult = salesInfo.leftOuterJoin(shopInfo).leftOuterJoin(score).
        map(t => (t._2._1._2.getOrElse("其他"), (t._1, t._2._1._1, t._2._2.getOrElse(500).toString.toDouble))).
        leftOuterJoin(betaInfo).filter( t => t._2._1._2._1 / 10000 >= t._2._2.get._2 && t._2._1._2._1 / 10000 < t._2._2.get._1 && t._2._1._3 >= t._2._2.get._4 && t._2._1._3 < t._2._2.get._3).
        map(t => (t._2._1._1, t._2._1._2._1, t._2._2.get._5, t._1, t._2._1._3, t._2._1._2._2, t._2._1._2._3, t._2._1._2._4)).
        map(t => (t._1, t._2, t._3, t._4, t._5, t._2 * t._3 / 12 * CreditService.shouXinMonths, CreditService.getRiskLimits(t._4, t._2).toDouble, t._6, t._7, t._8)).
        map(t => (t._1, (t._2, t._3, t._4, t._5, t._6, t._7, CreditService.productLimits, Array(t._6, t._7, CreditService.productLimits).min, t._8, t._9, t._10)))

      // Transformations过程：totalSales = 前12个月销售总额
      // (shop_id,totalSales)  // 例如 (7000,1.851615838000001E7)
      // =>  (shop_id,((         totalSales, totalSales/12*1.5),Some((         resultSales,  β, industry_type,score,sales*β/12*授信月份数, 行业风险限额,产品限额,MIN{resultSales*β/12*授信月份数,行业风险限额,产品限额}，大型活动销售额, 刷单比率,            退款率)))
      // 例如 (   7000,((1.851615838000001E7,2314519.7975000013),Some((1.6209981109168917E7,0.16,          其他,551.0,    1296798.4887335133,     250000.0,  500000,                                               250000.0,        10000.0,     0.12,0.4632258064516129))))
      // =>  (shop_id,          totalSales,         resultSales,  β, industry_type, score, resultSales*β/12*授信月份数,  totalSales/12*1.5, 行业风险限额, 产品限额,大型活动销售额, 刷单比率,            退款率, MIN{resultSales*β/12*授信月份数,totalSales/12*1.5,行业风险限额,产品限额})
      //     (   7000, 1.851615838000001E7, 1.851615838000001E7, 0.1,          其他,   0.0,      925807.9190000006, 2314519.7975000013,     250000.0,   500000,       10000.0,     0.12,0.4632258064516129,                                                            250000.0)
      // 最后输出的字段分别是：
      // #1# shop_id, #2# 前12个月销售总额，#3# [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]，#4# β，
      // #5# 行业类型，#6# 得分，#7# [(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]*β/12*授信月份数，#8# 前12个月销售总额/12*1.5，
      // #9# 行业风险限额, #10# 产品限额, #11# 大型活动销售额,( /*t._2._2.get._9, */ 去掉！) #12# 刷单比率,
      // #13# 退款率, #14# MIN{[(前12个月销售总额-大型活动产生的交易额)*(1-刷单比率)*（1-退款率）]*β/12*授信月份数,totalSales/12*1.5,行业风险限额,产品限额}
      val result = totalSales.map(t => (t._1, (t._2, t._2 / 12 * 1.5))).leftOuterJoin(tmpResult).map( t => (t._1, t._2._1._1, t._2._2.get._1, t._2._2.get._2, t._2._2.get._3, t._2._2.get._4,
        t._2._2.get._5, t._2._1._2, t._2._2.get._6, t._2._2.get._7, t._2._2.get._10, t._2._2.get._11, Array(t._2._1._2, t._2._2.get._8).min))
      result.toDF("shop_id", "前12个月销售总额", "resultSales", "β", "行业类型", "得分", "resultSales*β/12*授信月份数",
        "前12个月销售总额/12*1.5", "行业风险限额", "产品限额", "刷单比率", "退款率", "授信额度").write.parquet(Constants.OutputPath.CREDIT_PARQUET)
      result.map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile(Constants.OutputPath.CREDIT_TEXT)
        * */
    } catch {
      case t: Throwable =>
        // MailAgent(t, Constants.Mail.CREDIT_SUBJECT, Mail.getPropByKey("mail_to_credit")).sendMessage()
        logError(Constants.Mail.CREDIT_SUBJECT, t)
    }
  }
}

object CreditService {
  //授信额度上限(单位：元)
  val loanCeiling = 500000
  def getBeta(salesRateWeighting: Double, dayAverageSales: Double): Double = {
    if (salesRateWeighting > 2)
      getBetaByAvgSalAndRow(dayAverageSales, Array(1.50, 1.30, 1.20, 1.00, 0.90))
    else if (salesRateWeighting > 1.5 && salesRateWeighting <= 2)
      getBetaByAvgSalAndRow(dayAverageSales, Array(1.30, 1.10, 1.00, 0.80, 0.70))
    else if (salesRateWeighting > 1.2 && salesRateWeighting <= 1.5)
      getBetaByAvgSalAndRow(dayAverageSales, Array(1.20, 1.00, 0.90, 0.70, 0.60))
    else if (salesRateWeighting > 1 && salesRateWeighting <= 1.2)
      getBetaByAvgSalAndRow(dayAverageSales, Array(1.00, 0.90, 0.80, 0.60, 0.50))
    else if (salesRateWeighting > 0.8 && salesRateWeighting <= 1)
      getBetaByAvgSalAndRow(dayAverageSales, Array(0.90, 0.70, 0.60, 0.40, 0.20))
    else
      0D
  }

  private def getBetaByAvgSalAndRow(dayAverageSales: Double, betaRow: Array[Double]): Double = {
    if (dayAverageSales > 5000)
      betaRow(0)
    else if (dayAverageSales > 4000 && dayAverageSales <= 5000)
      betaRow(1)
    else if (dayAverageSales > 3000 && dayAverageSales <= 4000)
      betaRow(2)
    else if (dayAverageSales > 2000 && dayAverageSales <= 3000)
      betaRow(3)
    else if (dayAverageSales > 1000 && dayAverageSales <= 2000)
      betaRow(4)
    else
      0D
  }

}
