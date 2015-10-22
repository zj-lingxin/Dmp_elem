package com.asto.dmp.elem.dao

import org.apache.spark.rdd.RDD

object LoanWarningDao {
  /**
   * 基于3个月的营业额增长率（SaleRateInThreeMonths）
   */
  def getSaleRateInThreeMonths: RDD[(String,Double)] = AccessDao.getSaleRateInThreeMonths

  /**
   * 基于1个月的营业额增长率（SaleRateInOneMonths）
   *
   */
  def getSaleRateInOneMonths: RDD[(String,Double)] = AccessDao.getSaleRateInOneMonths
}
