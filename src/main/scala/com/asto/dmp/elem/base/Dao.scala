package com.asto.dmp.elem.base

import com.asto.dmp.elem.util.Utils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

trait Dao extends DataSource {
  protected def getProps(inputFilePath: String, schema: String, tempTableName: String, sqlObj: SQL, separator: String = Constants.InputPath.SEPARATOR) = {
    registerTempTableIfNotExist(inputFilePath, schema, tempTableName, separator)

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

    logInfo(Utils.wrapLog(s"执行Sql:${_sql}"))
    val rdd = sqlContext.sql(_sql).map(a => a.toSeq.toArray)

    if (Option(sqlObj.getOrderBy).isDefined) {
      //order by操作完成后设回默认值200
      logInfo(Utils.wrapLog("order by 操作完成,设回默认值: SET spark.sql.shuffle.partitions=200"))
      sqlContext.sql("SET spark.sql.shuffle.partitions=200")
    }
    rdd
  }

  private def registerTempTableIfNotExist(inputFilePath: String, schema: String, tempTableName: String, separator: String) {
    //如果临时表未注册，就进行注册
    if (!sqlContext.tableNames().contains(tempTableName)) {
      val fieldsNum = schema.split(",").length
      val rowRDD = Contexts.getSparkContext.textFile(inputFilePath)
        .map(_.split(separator)).filter(x => x.length == fieldsNum)
        .map(fields => for (field <- fields) yield field.trim)
        .map(fields => Row(fields: _*))
      sqlContext.createDataFrame(rowRDD, simpleSchema(schema)).registerTempTable(tempTableName)
      logInfo(Utils.wrapLog(s"注册临时表：$tempTableName"))
    }
  }

  private def simpleSchema(schemaStr: String): StructType = {
    StructType(schemaStr.toLowerCase.split(",").map(fieldName => StructField(fieldName.trim, StringType, nullable = true)))
  }

}
