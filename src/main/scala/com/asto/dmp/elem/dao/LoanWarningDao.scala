package com.asto.dmp.elem.dao

object LoanWarningDao {
  /**
   * 基于3个月的营业额增长率（SaleRateInThreeMonths）
   */
  def getSaleRateInThreeMonths = AccessDao.getSaleRateInThreeMonths

  /**
   * 基于1个月的营业额增长率（SaleRateInOneMonths）
   *
   */
  def getSaleRateInOneMonths = AccessDao.getSaleRateInOneMonths
}
