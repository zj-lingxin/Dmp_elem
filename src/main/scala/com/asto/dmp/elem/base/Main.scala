package com.asto.dmp.elem.base

import com.asto.dmp.elem.service._
import org.apache.spark.Logging


object Main extends Logging {
  def main(args: Array[String]) {
    if (args == null || args.length == 0) {
      logError("请传入模型编号：001~005")
      return
    }

    args(0) match {
      case "001" =>
        //准入模型
        logInfo(s"开始运行准入模型(001)")
        new AccessService().run()
      case "002" =>
        //反欺诈模型
        logInfo(s"开始运行反欺诈模型(002)")
        new AntiFraudService().run()
      case "003" =>
        //评分模型
        //暂无
      case "004" =>
        //授信模型
        logInfo(s"开始运行授信模型(004)")
        new CreditService().run()
      case "005" =>
        //贷后模型
        logInfo(s"开始运行贷后模型(005)")
        new LoanAfterService().run()
      case _     =>
        logError(s"传入参数错误!传入的是${args(0)},请传入001~005")
    }

    BaseContext.stopSparkContext()
    System.exit(0)
  }
}
