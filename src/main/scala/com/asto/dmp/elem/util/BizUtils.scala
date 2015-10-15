package com.asto.dmp.elem.util

import com.asto.dmp.elem.base.{Constants, BaseContext}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

/**
 * 该类中定义的是跟业务相关的一些共用方法。这些方法必须是在这个项目中自己能够用到的，并且其他同事也可能用到的方法。
 * 注意：如果这些方法不仅仅在该项目中能用到，而且可能在未来的项目中也能用到，那么请写到Utils中
 */
object BizUtils {

  def registerOrder: Unit ={

  }


  //对RDD的元素是二元组的RDD提供自定义函数的功能
  class RichPairRDD[K: Ordering : ClassTag, V](rdd: RDD[_ <: Product2[K, V]]) {
    //将子行业类型作为二元组的第一个元素，其他字段作为第二个元素，调用该方法后可以生成一个三元组，三元组中多出了父行业分类。
    //例如：(3C数码,(2c01b6044e0f48348251fca53bb714fa,美的天天购专卖店)) -> (3C数码,3C,(2c01b6044e0f48348251fca53bb714fa,美的天天购专卖店))
    def addIndustryType(): RDD[(String, String, V)] = {
      BaseContext.getSparkContext.textFile(Constants.MiddlePath.INDUSTRY_RELATION).
        map(_.split(",")).filter(_.length == 2).map(a => (a(0).trim, a(1).trim)).
        rightOuterJoin(rdd.asInstanceOf[RDD[(String, V)]]).map(t => (t._1, t._2._1.getOrElse("其他"), t._2._2.asInstanceOf[V]))
    }
  }
  implicit def rdd2RichPairRDD[K: Ordering : ClassTag, V](rdd: RDD[_ <: Product2[K, V]]): RichPairRDD[K, V] = new RichPairRDD(rdd)
}
