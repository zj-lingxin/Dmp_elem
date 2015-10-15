package com.asto.dmp.elem.dao

import com.asto.dmp.elem.base.DataSource

/**
 * Created by lingx on 2015/10/14.
 */
class BizDao extends DataSource {
  val queryString = "id,user_id"
  //  def orderProperty(query: String) = {
  //    if (properties == "*") {
  //
  //    } else {
  //
  //    }
  //    sqlContext.sql(s"select $properties from order ").map(a => (a(0).toString, a(1)))
  //  }


}
object BizDao extends DataSource{
  def toTuple[A <: Object ](as:Seq[A]) = {
    val tupleClass = Class.forName("scala.Tuple" + as.size)
    tupleClass.getConstructors.apply(0).newInstance(as:_*).asInstanceOf[Product]
  }

  private def foo(selectedProps: String) = {
    val (indexMap,typeMap) = getIndexAndTypeMaps(CREDIT_TRADE_VIEW)
    val propNames = selectedProps.split(",")

    //sqlContext.sql(s"select $selectedProps from order ").map(a => toTuple(a.toSeq))
    List[List[String]](List("1","ss"),List("2","ss1")).map(a => toTuple(a.toSeq)).foreach(println)
  }

  private def getIndexAndTypeMaps(schema: String) = {
    val indexMap = scala.collection.mutable.Map[String,Int]()
    val typeMap  = scala.collection.mutable.Map[String,String]()
    val elems = schema.split(",")
    var index = 0
    for(e <- elems){
      val sp = e.split(":")
      val propertyName = sp(0).trim
      val propertyType = sp(1).trim
      indexMap += (propertyName -> index)
      index += 1
      typeMap += (propertyName -> propertyType)
    }
    (indexMap,typeMap)
  }
  val CREDIT_TRADE_VIEW    = "id:String,user_id:Long,shop_id:String,gmt_target:Date,pay_money:Double"
  def main(args: Array[String]) {
    foo("id,shop_id")
  }
}
