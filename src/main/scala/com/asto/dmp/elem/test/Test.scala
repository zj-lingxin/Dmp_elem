package com.asto.dmp.elem.test

import com.asto.dmp.elem.base.Contexts
import com.asto.dmp.elem.util.FileUtils

/**
 * 无关紧要的代码。可删除。
 */
object Test {
  //remember delete!!
  def tempConvertData(): Unit = {
    FileUtils.deleteHdfsFiles("hdfs://appcluster/elem/input/order")
    Contexts.getSparkContext.textFile("hdfs://appcluster/elem/input/order2").map(_.split("\t")).filter(_.length == 13).map(a => (foo(a(0)),a(1),a(2),a(3),a(4),a(5),a(6),a(7),a(8),foo(a(9)),a(10),a(11),a(12))).map(_.productIterator.mkString("\t")).coalesce(1).saveAsTextFile("hdfs://appcluster/elem/input/order")
  }
  def foo(strDate: String) = {
    if(strDate.contains("2015/4")) {
      strDate.replace("2015/4","2015/9")
    } else if(strDate.contains("2014/12") && strDate < "2014/12/22") {
      strDate.replace("2014/12","2015/10")
    } else if(strDate.contains("2015/1") && strDate > "2015/1/13") {
      strDate.replace("2015/1","2015/8")
    } else {
      strDate
    }
  }

  //remember delete!!
  def testTempConvert() = {
    val rdd = Contexts.getSparkContext.textFile("hdfs://appcluster/elem/input/order").map(_.split("\t"))
      .filter(a => a(0).contains("2015/9") || a(0).contains("2015/10")).map(_.toIterator.mkString(","))foreach(println)

  }

  def main(args: Array[String]) {
    tempConvertData()
    testTempConvert()
  }
}
