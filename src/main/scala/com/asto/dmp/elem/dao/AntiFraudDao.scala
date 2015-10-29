package com.asto.dmp.elem.dao

import java.util.Calendar
import com.asto.dmp.elem.base.{Constants, Contexts, SQL}
import com.asto.dmp.elem.util.{Utils, DateUtils, BizUtils}
import org.apache.spark.Logging

object AntiFraudDao extends Logging {
  val last12MothsList = BizUtils.getLastMonths(12, Constants.App.YEAR_MONTH_FORMAT)

  /**
   * FQZ1=订单额/近12个月订单额均值
   * 返回的字段是：(shop_id,订单号,FQZ1,true/false)
   * 刷单判定值: FQZ1>2.5
   */
  def getFQZ1Info = {
    val last12MonthsOrderSales = BizDao.getOrderProps(SQL().select("order_date,shop_id,order_money,order_id"))
      .map(a => (DateUtils.cutYearMonth(a(0).toString), a(1), a(2).toString.toDouble, a(3)))
      .filter(t => last12MothsList.contains(t._1)) // 过滤出近12个月的数据 2014/10,15453,14.0,12774264860537753)
    val last12MonthsAvgSales = last12MonthsOrderSales
        .map(t => (t._2, t._3)).groupByKey() //(15453,CompactBuffer(74.0, 22.0, 7.0, 12.0, 13.0....))
        .map(t => (t._1, t._2.sum / t._2.toList.length)) //求出近12个月定单额均值：(15453,16.351623407983883)
    last12MonthsOrderSales.map(t => (t._2, (t._4, t._3)))
      .leftOuterJoin(last12MonthsAvgSales) //(15453,((12974166848017753,15.0),Some(16.351623407983883)))
      .map(t => (t._1, t._2._1._1, t._2._1._2 / t._2._2.get, (t._2._1._2 / t._2._2.get) > 2.5)) //(shop_id,订单号,FQZ1)(15453,12674266136499453,1.0090741199398996)
  }

  /**
   * FQZ2=订单额/所在季度同期订单额均值（如第二季度第3周周一的订单，就和第二季度所有周一的单笔订单额均值进行比较）
   * 返回字段：(店铺ID：15453,订单ID：12862924022113753,FQZ2：0.9322091675567575)
   * 刷单判定值 FQZ2 > 3
   */
  def getFQZ2Info = {
    val dataRDD = BizDao.getOrderProps(SQL().select("order_id,order_date,shop_id,order_money"))
      .filter(a => last12MothsList.contains(DateUtils.cutYearMonth(a(1).toString))) //过滤出近12份个月的数据
      .map(a => (a(0).toString, a(1).toString, a(2).toString, a(3).toString.toDouble, getWeekDay(a(1).toString), DateUtils.getQuarterStartTime(a(1).toString, Constants.App.YEAR_MONTH_DAY_FORMAT))).cache()

    val avgSalesRDD = dataRDD.groupBy(t => (t._6, t._3, t._5)) //按季度、shop_id、星期几排序 ((2015-07-01,1,7),CompactBuffer((11211,2015-07-11,1,23.0,7,2015-07-01), (11215,2015-09-12,1,22.3,7,2015-07-01)))
      .map(t => (t._1, count(t._2.toIterator)))

    val tempRDD = dataRDD.map(t => ((t._6, t._3, t._5), t))
    avgSalesRDD.leftOuterJoin(tempRDD)
      .map(t => (t._2._2.get, t._2._1)) //((订单编号 12268923781587153, 订单日期：2015/4/16, shop_id: 15453, 订单额：15.0,星期几(注意星期日是1，星期一是2，以此类推)5,季度：2015/4/1),所在季度同期订单额均值：17.28598174584601)
      .map(t => (t._1._3, t._1._1, t._1._4 / t._2, (t._1._4 / t._2) > 3))
  }

  /**
   * 复购率:FQZ3=同一买家ID一天之内的购买次数
   * 返回字段：(订单ID：12867796649165953,下单日期：2015/4/11,店铺ID：15453,客户ID：4014872,FQZ3,FQZ3是否大于2)
   */
  def getFQZ3Info = {
    val dataRDD = BizDao.getOrderProps(SQL().select("order_id,order_date,shop_id,custom_id"))
      .filter(a => last12MothsList.contains(DateUtils.cutYearMonth(a(1).toString))) //过滤出近12份个月的数据
      .map(a => ((a(1).toString, a(2).toString, a(3).toString), a(0).toString))
    val repeatBuyKey = dataRDD.groupByKey().map(t => (t._1, t._2.size))
    dataRDD.leftOuterJoin(repeatBuyKey) //((2014/11/14,15453,5589346),(12159697518643953,Some(2)))
      .map(t => (t._2._1, t._1._1, t._1._2, t._1._3, t._2._2.getOrElse(1), (t._2._2.getOrElse(1)) > 2))
  }

  /**
   * 下单距离 FQZ4<0.1KM or FQZ4>5KM 则下单距离异常
   * 返回字段：(店铺ID：15453,订单ID：12774759155019053,下单距离：0.9928379964629559,是否下单距离异常:false)
   */
  def getFQZ4Info = {
    //注意：店铺经纬度风控还未给出 需要加入真数据~~~~~~~~~~~需要加入真数据~~~~~~~~~~需要加入真数据,重要的事说三遍！
    val shopRDD = Contexts.getSparkContext.parallelize(Seq(
      ("15453", "120.163436", "30.326016"), //风云便当
      ("17644", "120.026576", "30.216287"),
      ("98492", "120.205626", "30.266114"), //三二行馆
      ("186049", "120.16382217407227", "30.29351234436035"), //黄焖鸡米饭
      ("19902", "120.139288", "30.281015"), //【赠饮赠鸡腿】传家美膳
      ("19483", "120.02374649047852", "30.21042823791504"), //小米便当
      ("43542", "120.206797", "30.256218"), //945就是我外卖
      ("7433", "120.351948", "30.324667") //米宝宝
    )).map(t => (t._1.toString, (t._2, t._3)))
    BizUtils.getLastMonths(12, Constants.App.YEAR_MONTH_FORMAT)
    val unFilterRDD = BizDao.getOrderProps(SQL().select("order_id,order_date,shop_id,custom_id,lng_lat"))
      .filter(a => last12MothsList.contains(DateUtils.cutYearMonth(a(1).toString))) //过滤出近12份个月的数据
      .map(a => {val (lng, lat) = getLngAndLat(a(4).toString); (a(2).toString, (a(0).toString, a(1).toString, a(3).toString, lng, lat))})
      .leftOuterJoin(shopRDD).cache() //(15453,((12974166848017753,2015/8/13,1815187,120.15386581420898,30.318574905395508),Some((120.163436,30.326016))))
    val noLngLatShops = unFilterRDD.filter(t => t._2._2.isEmpty).map(_._1).distinct().collect()
    if (noLngLatShops.length > 0) {
      logWarning(Utils.wrapLog(s"店铺Id为 ${noLngLatShops.mkString(",")} 的店铺缺少店铺坐标，所以过滤掉"))
    }
    unFilterRDD.filter(t => t._2._2.isDefined)
      .map(t => (t._1, t._2._1._1, getDistance(t._2._1._4.toDouble, t._2._1._5.toDouble, t._2._2.get._1.toDouble, t._2._2.get._2.toDouble)))
      .map(t => (t._1, t._2, t._3, t._3.toDouble < 0.1 || t._3.toDouble > 5))
  }

  /**
   * FQZ5=订单额/当日订单额均值（注：不同时点，阀值不同。）
   * FQZ5≥2.45  繁忙时点（10,11,12,16,17,18,19）
   * FQZ5≥2.55 其他闲暇时点
   * (12766260704412053,15.0,2015/3/17,15453,风云便当,2015/3/17 10:51,0.9799346710219319,false)
   * 返回字段：(订单号,订单额,订单日期,店铺ID,店铺名称,下单时间,FQZ5,是否异常)
   */
  def getFQZ5Info = {
    val last12MonthsOrderSales = BizDao.getOrderProps(SQL().select("order_date,shop_id,order_money,order_id,place_order_time,shop_name"))
      .map(a => (a(0).toString, a(1), a(2).toString.toDouble, a(3), a(4), a(5)))
      .filter(t => last12MothsList.contains(DateUtils.cutYearMonth(t._1.toString))) // 过滤出近12个月的数据 (2015/8/13,15453,16.0,12174964992731653)
    val avgSaleInDay = last12MonthsOrderSales
        .map(t => ((t._1, t._2.toString), t._3.toDouble))
        .groupByKey() //((2015/6/21,15453),CompactBuffer(18.0, 18.0, 17.0, 15.0, 18.0, ...))
        .map(t => (t._1, t._2.sum / t._2.toList.length)) //((2014/12/1,15453),16.46951219512195)
    last12MonthsOrderSales.map(t => ((t._1, t._2.toString), (t._3, t._4, t._5, t._6)))
      .leftOuterJoin(avgSaleInDay) //((2015/3/17,15453),((15.0,12666561137601753,2015/3/17 16:46),Some(15.307142857142857)))
      .filter(t => t._2._2.isDefined)
      .map(t => {
      val FQZ5 = t._2._1._1.toString.toDouble / t._2._2.get; (t._2._1._2, t._2._1._1, t._1._1, t._1._2, t._2._1._4, t._2._1._3.toString, FQZ5, AntiFraudDao.isFraudInFQZ5(FQZ5, t._2._1._3.toString))
    })
  }

  /**
   * 获取下单距离的方法
   * lng经度，lat纬度
   * val sin1 = SIN((90-订单纬度)*PI()/180)
   * val sin2 = SIN((90-店铺纬度)*PI()/180)
   * val sin3 = SIN(订单经度*PI()/180)
   * val sin4 = SIN(店铺经度*PI()/180)
   * val cos1 = COS((90-订单纬度)*PI()/180)
   * val cos2 = COS((90-店铺纬度)*PI()/180)
   * val cos3 = COS(订单经度*PI()/180)
   * val cos4 = COS(店铺经度*PI()/180)
   * A = sin1 * cos3 - sin2 * cos4
   * B = sin1 * sin3 - sin2 * sin4
   * C = cos1 - cos2
   * D = 6371.004 * arccos(1 - (Power(A,2)+Power(B,2)+Power(C,2))/2)
   * @param orderLng 订单经度
   * @param orderLat 订单纬度
   * @param shopLng 店铺经度
   * @param shopLat 店铺纬度
   */
  def getDistance(orderLng: Double, orderLat: Double, shopLng: Double, shopLat: Double) = {
    val sin1 = Math.sin((90 - orderLat) * Math.PI / 180)
    val sin2 = Math.sin((90 - shopLat) * Math.PI / 180)
    val sin3 = Math.sin(orderLng * Math.PI / 180)
    val sin4 = Math.sin(shopLng * Math.PI / 180)
    val cos1 = Math.cos((90 - orderLat) * Math.PI / 180)
    val cos2 = Math.cos((90 - shopLat) * Math.PI / 180)
    val cos3 = Math.cos(orderLng * Math.PI / 180)
    val cos4 = Math.cos(shopLng * Math.PI / 180)
    val a = sin1 * cos3 - sin2 * cos4
    val b = sin1 * sin3 - sin2 * sin4
    val c = cos1 - cos2
    6371.004 * Math.acos(1 - (Math.pow(a, 2) + Math.pow(b, 2) + Math.pow(c, 2)) / 2)
  }

  private def count(itr: Iterator[(String, String, String, Double, Int, String)]) = {
    var sum: Double = 0
    var count: Int = 0
    itr.foreach(a => {
      sum += a._4
      count += 1
    })
    if (count != 0) sum / count else 0D
  }

  private def getWeekDay(strDate: String): Int = {
    DateUtils.strToCalendar(strDate, Constants.App.YEAR_MONTH_DAY_FORMAT).get(Calendar.DAY_OF_WEEK)
  }

  /**
   * 将"[纬度，经度]"形式的字符串进行去中括号，且分割
   * @param lnglat
   * @return (经度,纬度)
   */
  private def getLngAndLat(lnglat: String) = {
    val ary = """[\[\"\]]""".r.replaceAllIn(lnglat, "").split(",")
    (ary(1).trim, ary(0).trim)
  }

  private def isFraudInFQZ5(FQZ5Value: Double, placeOrderTime: String) = {
    // 取出“2015/3/17 17:33”中的小时
    val placeOrderHour: Int = (placeOrderTime.split(" ")(1)).split(":")(0).toInt

    //闲暇时的FQZ5上限
    var FQZ5Line = 2.55
    if (List(10, 11, 12, 16, 17, 18, 19).contains(placeOrderHour)) {
      //繁忙时的FQZ5上限
      FQZ5Line = 2.45
    }
    FQZ5Value >= FQZ5Line
  }

}