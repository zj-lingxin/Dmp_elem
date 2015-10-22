package com.asto.dmp.elem.base

import org.apache.spark.Logging

trait DataSource extends Logging with scala.Serializable {
  protected val sc = Contexts.getSparkContext
  //使用lazy,当确实要使用HiveContext或者SqlContext时再去初始化
  protected lazy val sqlContext = Contexts.getSqlContext
  protected lazy val hiveContext = Contexts.getHiveContext
}
