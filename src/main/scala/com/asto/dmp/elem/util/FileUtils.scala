package com.asto.dmp.elem.util

import com.asto.dmp.elem.base.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
 * 文件相关的工具类
 */
object FileUtils extends Logging {
  def deleteHdfsFiles(paths: String*) = {
    paths.foreach { path =>
      val filePath = new Path(path)
      val hdfs = filePath.getFileSystem(new Configuration())
      if (hdfs.exists(filePath)) {
        logInfo(Utils.wrapLog(s"删除目录：$filePath"))
        hdfs.delete(filePath, true)
      }
    }
  }

  def saveAsTextFile[T <: Product](rdd: RDD[T], savePath: String, deleteExistingFiles: Boolean = true) = {
    if(deleteExistingFiles)
      deleteHdfsFiles(savePath)
    rdd.map(_.productIterator.mkString(Constants.OutputPath.SEPARATOR)).coalesce(1).saveAsTextFile(savePath)
  }
}
