package com.asto.dmp.elem.base

import java.util.Calendar

import com.asto.dmp.elem.service._
import com.asto.dmp.elem.util.{DateUtils, Utils}
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    if (Option(args).isEmpty || args.length == 0) {
      logError(Utils.wrapLog("请传入模型编号：001~005"))
      return
    }
    args(0) match {
      case "001" =>
        //反欺诈模型
        logInfo(Utils.wrapLog(s"开始运行反欺诈模型(${args(0)})"))
        new AntiFraudService().run()
      case "002" =>
        //准入模型
        logInfo(Utils.wrapLog(s"开始运行准入模型(${args(0)})"))
        new AccessService().run()
      case "003" =>
        //授信模型
        logInfo(Utils.wrapLog(s"开始运行授信模型(${args(0)})"))
        new CreditService().run()
      case "004" =>
        //贷后模型
        logInfo(Utils.wrapLog(s"开始运行贷后模型(${args(0)})"))
        new LoanAfterService().run()
      case _ =>
        logError(s"传入参数错误!传入的是${args(0)},请传入001~004")
    }

    BaseContext.stopSparkContext()
    System.exit(0)
  }
}