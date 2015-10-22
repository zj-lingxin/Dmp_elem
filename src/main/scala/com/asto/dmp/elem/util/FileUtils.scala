package com.asto.dmp.elem.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * 文件相关的工具类
 */
object FileUtils {
  def deleteHdfsFiles(paths: String *) = {
    paths.foreach { path =>
      val filePath = new Path(path)
      val hdfs = filePath.getFileSystem(new Configuration())
      if (hdfs.exists(filePath))  hdfs.delete(filePath, true)
    }
  }
}
