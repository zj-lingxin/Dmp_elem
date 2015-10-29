package com.asto.dmp.elem.base

import com.asto.dmp.elem.service._
import com.asto.dmp.elem.util.Utils
import org.apache.spark.Logging

/**
 * 注意：店铺经纬度风控还未给出 需要加入真数据~~~~~~~~~~~需要加入真数据~~~~~~~~~~需要加入真数据,重要的事说三遍！
 */
object Main extends Logging {


  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    if (Option(args).isEmpty || args.length == 0) {
      logError(Utils.wrapLog("请传入模型编号：1~5"))
      return
    }

    runServicesByArgs(args(0))

    Contexts.stopSparkContext()

    val endTime = System.currentTimeMillis()

    logInfo(s"程序共运行${(endTime - startTime) / 1000}秒")

  }

  private def runServicesByArgs(arg: String) = {
    arg match {
      case "1" =>
        //反欺诈模型
        new AntiFraudService().run()
      case "2" =>
        //准入模型
        new AccessService().run()
      case "3" =>
        //授信模型
        new CreditService().run()
      case "4" =>
        //贷后模型
        new LoanWarningService().run()
      case "5" =>
        //所有模型一起运行
        logInfo(Utils.wrapLog("所有模型一起运行"))
        new AntiFraudService().run()
        new AccessService().run()
        new CreditService().run()
        new LoanWarningService().run()
      case _ =>
        logError(s"传入参数错误!传入的是${arg},请传入1~5")
    }
  }
}