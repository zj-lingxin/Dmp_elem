package com.asto.dmp.elem.base

class SQL(var select: String, var where: String) {
  var orderBy: String = _
  var limit: Integer = _

  def this(select: String) {
    this(select, null)
  }

  def this() {
    this("*")
  }

  def select(select: String): this.type = {
    this.select = select
    this
  }

  def where(where: String): this.type = {
    this.where = where
    this
  }

  def orderBy(orderBy: String): this.type = {
    this.orderBy = orderBy
    this
  }

  def limit(limit: Integer): this.type = {
    this.limit = limit
    this
  }
  
}

object SQL {
  def apply() = new SQL()
  def apply(select: String) = new SQL(select)
  def apply(select: String, where: String) = new SQL(select, where)
}