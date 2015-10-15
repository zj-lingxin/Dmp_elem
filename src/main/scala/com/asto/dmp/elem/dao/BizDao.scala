package com.asto.dmp.elem.dao

import com.asto.dmp.elem.base.{SQL, Constants, BaseContext, DataSource}
import com.asto.dmp.elem.util.Utils
import org.apache.spark.sql.Row

object BizDao extends DataSource {

  private def getProps(inputFilePath: String, schema: String, tempTableName: String, sqlObj: SQL) = {
    val fields = schema.split(",")
    val rowRDD = BaseContext.getSparkContext.textFile(inputFilePath).
      map(_.split(Constants.App.SEPARATOR)).filter(x => x.length == fields.length).
      map(fields => for (field <- fields) yield field.trim).map(fields => Row(fields: _*))
    sqlContext.createDataFrame(rowRDD, getSchema(schema)).registerTempTable(tempTableName)

    var _sql = s"SELECT ${sqlObj.getSelect} FROM $tempTableName"

    if (Option(sqlObj.getWhere).isDefined)
      _sql += s" WHERE ${sqlObj.getWhere}"

    if (Option(sqlObj.getOrderBy).isDefined) {
      //使用OrderBy的时候，需要将spark.sql.shuffle.partitions设小
      sqlContext.sql(s"SET spark.sql.shuffle.partitions=10")
      logInfo(Utils.wrapLog("order by 操作需要设置: SET spark.sql.shuffle.partitions=200 "))
      _sql += s" ORDER BY  ${sqlObj.getOrderBy} "
    }

    //暂时不支持 group by，请使用相关的Transforamtion操作

    if (Option(sqlObj.getLimit).isDefined)
      _sql += s" LIMIT ${sqlObj.getLimit}"

    logInfo(Utils.wrapLog(s"执行Sql:${_sql} ####################"))
    val rdd = sqlContext.sql(_sql).map(a => Utils.toTuple(a.toSeq.asInstanceOf[Seq[String]]))

    if (Option(sqlObj.getOrderBy).isDefined) {
      //order by操作完成后设回默认值200
      logInfo(Utils.wrapLog("order by 操作完成,设回默认值: SET spark.sql.shuffle.partitions=200"))
      sqlContext.sql("SET spark.sql.shuffle.partitions=200")
    }
    rdd
  }

  def getOrderProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.ORDER, Constants.Schema.ORDER, "orderTable", sql)

}
