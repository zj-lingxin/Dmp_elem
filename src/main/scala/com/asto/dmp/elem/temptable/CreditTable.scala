package com.asto.dmp.elem.temptable

import com.asto.dmp.elem.base.{BaseContext, Constants, DataSource}
import org.apache.spark.sql.Row

object CreditTable extends DataSource with scala.Serializable {
  def registerOrder() = {
    val orderRowRDD = BaseContext.getSparkContext.textFile(Constants.InputPath.ORDER).
      map(_.split(Constants.App.SEPARATOR)).filter(x => x.length == 14).map(a => Row(a)).collect()
    //BaseContext.getSqlContext.createDataFrame(orderRowRDD, getSchema(Constants.Schema.ORDER)).registerTempTable("order")
  }
}


