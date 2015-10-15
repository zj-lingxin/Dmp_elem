package com.asto.dmp.elem.util

import com.asto.dmp.elem.base.Constants

/**
 * 该类中定义的是跟项目的业务无关的一些共用方法。这些方法放入到DateUtils和FileUtils中是不合适的。
 * 这些方法必须具有通用性。自己能够用到的，并且其他同事也可能用到的方法。且在未来的项目中也能使用。
 */
object Utils {

  /**
   * convert Seq to Tuple
   * you can use this method to convert List to Tuple,or Array to Tuple, etc.
   * It's worth noting that: toTuple(List(111, 222)) is Error, because Int is not the subclass of Object.
   * but toTuple(List[Integer](111, 222)) is ok
   * @param seq
   * @tparam A
   * @return
   */
  def toTuple[A <: Object](seq: Seq[A]): Product = {
    val tupleClass = Class.forName("scala.Tuple" + seq.size)
    tupleClass.getConstructors.apply(0).newInstance(seq: _*).asInstanceOf[Product]
  }

  def trimIterable[A <: Iterable[String]](iterable: A): A= {
    iterable.map(_.trim).asInstanceOf[A]
  }

  def wrapLog(log: String) = {
    s"${Constants.App.LOG_WRAPPER} $log ${Constants.App.LOG_WRAPPER}"
  }

}
