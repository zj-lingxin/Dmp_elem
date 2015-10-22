package com.asto.dmp.elem.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging

/**
 * 文件相关的工具类
 */
object FileUtils extends Logging {
  def deleteHdfsFiles(paths: String *) = {
    paths.foreach { path =>
      val filePath = new Path(path)
      val hdfs = filePath.getFileSystem(new Configuration())
      if (hdfs.exists(filePath)) {
        logInfo(Utils.wrapLog(s"删除目录：$filePath"))
        hdfs.delete(filePath, true)
      }
    }
  }
}
