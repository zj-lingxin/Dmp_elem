package com.asto.dmp.elem.base


import org.apache.spark.Logging
import org.apache.spark.sql.types._

trait DataSource extends Logging with scala.Serializable {
  protected val sc = BaseContext.getSparkContext
  //使用lazy,当确实要使用HiveContext或者SqlContext时再去初始化
  protected lazy val sqlContext = BaseContext.getSqlContext
  protected lazy val hiveContext = BaseContext.getHiveContext

  def getSchema(schemaStr: String): StructType = {

    StructType(schemaStr.toLowerCase.split(",").map(fieldName => StructField(fieldName.split(":")(0).trim,
      fieldName.split(":")(1).trim match {
        case "double" => DoubleType
        case "string" => StringType
        case "long" => LongType
        case "int" => IntegerType
        case "integer" => IntegerType
        case "float" => FloatType
        case _ => logError("代码有误：DataSourcegetSchema中没有相应的Type");null
      }
      , nullable = true)))
  }
}
