package com.asto.dmp.elem.dao

import com.asto.dmp.elem.base._

object BizDao extends Dao {

  def getOrderProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.ORDER, Constants.Schema.ORDER, "orderTable", sql)

  def getFakedInfoProps(sql: SQL = new SQL()) = getProps(Constants.OutputPath.ANTI_FRAUD_FAKED_INFO_TEXT, Constants.Schema.FAKED_INFO, "fakedInfo", sql, Constants.OutputPath.SEPARATOR)

  def getFakedRateProps(sql: SQL = new SQL()) = getProps(Constants.OutputPath.ANTI_FRAUD_FAKED_RATE_TEXT, Constants.Schema.FAKED_RATE, "fakedRate", sql, Constants.OutputPath.SEPARATOR)

  def getAccessInfoProps(sql: SQL = new SQL()) = getProps(Constants.OutputPath.ACCESS_TEXT, Constants.Schema.ACCESS_INFO, "accessInfo", sql, Constants.OutputPath.SEPARATOR)
}
