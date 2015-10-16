package com.asto.dmp.elem.util

import com.asto.dmp.elem.base.Constants

/**
 * 该类中定义的是跟项目的业务无关的一些共用方法。这些方法放入到DateUtils和FileUtils中是不合适的。
 * 这些方法必须具有通用性。自己能够用到的，并且其他同事也可能用到的方法。且在未来的项目中也能使用。
 */
object Utils {

  /**
   * Convert Seq to Tuple
   * you can use this method to convert List to Tuple,or Array to Tuple, etc.
   * It's worth noting that: toTuple(List(111, 222)) is Error, because Int is not the subclass of Object.
   * but toTuple(List[Integer](111, 222)) is ok
   * @param seq
   * @tparam A
   * @return
   */
  def toProduct[A <: Object](seq: Seq[A]) =
    Class.forName("scala.Tuple" + seq.size).getConstructors.apply(0).newInstance(seq: _*).asInstanceOf[Product]


  /**
   * Use trim() method for every element of Iterable,and return the result
   * @param iterable
   * @tparam A
   * @return
   */
  def trimIterable[A <: Iterable[String]](iterable: A): A = {
    iterable.map(_.trim).asInstanceOf[A]
  }

  def trimTuple(x: Product) = toProduct((for (e <- x.productIterator) yield {
    e.toString.trim
  }).toList)

  /**
   * Add a ${Constants.App.LOG_WRAPPER} in the log header and tail
   * @param log log Information
   * @return
   */
  def wrapLog(log: String) = {
    s"${Constants.App.LOG_WRAPPER} $log ${Constants.App.LOG_WRAPPER}"
  }

  class IteratorToTuple[A](elements: Iterable[A]) {
    def toTuple1  = elements match { case Seq(a) => (a) }
    def toTuple2  = elements match { case Seq(a, b) => (a, b) }
    def toTuple3  = elements match { case Seq(a, b, c) => (a, b, c) }
    def toTuple4  = elements match { case Seq(a, b, c, d) => (a, b, c, d) }
    def toTuple5  = elements match { case Seq(a, b, c, d, e) => (a, b, c, d, e) }
    def toTuple6  = elements match { case Seq(a, b, c, d, e, f) => (a, b, c, d, e, f) }
    def toTuple7  = elements match { case Seq(a, b, c, d, e, f, g) => (a, b, c, d, e, f, g) }
    def toTuple8  = elements match { case Seq(a, b, c, d, e, f, g, h) => (a, b, c, d, e, f, g, h) }
    def toTuple9  = elements match { case Seq(a, b, c, d, e, f, g, h, i) => (a, b, c, d, e, f, g, i) }
    def toTuple10 = elements match { case Seq(a, b, c, d, e, f, g, h, i, j, k) => (a, b, c, d, e, f, g, i, j, k) }
    def toTuple11 = elements match { case Seq(a, b, c, d, e, f, g, h, i, j, k, l) => (a, b, c, d, e, f, g, h, i, j, k, l) }
    def toTuple12 = elements match { case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m) => (a, b, c, d, e, f, g, h, i, j, k, l, m) }
    def toTuple13 = elements match { case Seq(a, b, c, d, e, f, g, h, i, j, k, l, m, n) => (a, b, c, d, e, f, g, h, i, j, k, l, m, n) }
  }

  implicit def iteratorToTuple[A](elements: Iterable[A]) = new IteratorToTuple(elements.toSeq)
  implicit def iteratorToTuple[A](elements: Array[A]) = new IteratorToTuple(elements.toSeq)
}
