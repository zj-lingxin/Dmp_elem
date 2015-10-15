package com.asto.dmp.elem.base


class SQL {
  //暂时不支持 _groupBy
  private var _select: String = "*"
  private var _where: String = _
  private var _orderBy: String = _
  private var _limit: Integer = _

  def setSelect(select: String): this.type = {
    this._select = select
    this
  }

  def getSelect = _select

  def setWhere(where: String): this.type = {
    this._where = where
    this
  }

  def getWhere = _where

  def setOrderBy(orderBy: String): this.type = {
    this._orderBy = orderBy
    this
  }

  def getOrderBy = _orderBy

  def setLimit(limit: Integer): this.type = {
    this._limit = limit
    this
  }

  def getLimit = _limit
}
