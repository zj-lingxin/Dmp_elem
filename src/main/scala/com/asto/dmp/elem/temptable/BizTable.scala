package com.asto.dmp.elem.temptable

import com.asto.dmp.elem.base.{Constants, BaseContext}
import com.asto.dmp.elem.temptable.CreditTable._
import org.apache.spark.sql.Row

object BizTable {
  def registerOrder() = {
    val orderRowRDD = BaseContext.getSparkContext.textFile(Constants.InputPath.ORDER).
      map(_.split(Constants.App.SEPARATOR)).filter(x => x.length == 14).map(a => Row(a))
    BaseContext.getSqlContext.createDataFrame(orderRowRDD, getSchema(Constants.Schema.ORDER)).registerTempTable("order")
  }
}
