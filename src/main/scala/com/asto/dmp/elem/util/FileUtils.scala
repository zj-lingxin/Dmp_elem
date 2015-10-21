package com.asto.dmp.elem.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
 * 文件相关的工具类
 */
object FileUtils {
  def writeToTextFile(rdd: RDD[Row], path: String): Unit = {
    scala.tools.nsc.io.File(path).appendAll("\n" + rdd.collect().mkString("\n"))
  }

  def writeToParquetFile(data: DataFrame, path: String): Unit = {
    data.write.mode(SaveMode.Append).format("parquet").save("path")
  }

  def deleteHdfsFiles(paths: String *) = {
    paths.foreach { path =>
      val filePath = new Path(path)
      val hdfs = filePath.getFileSystem(new Configuration())
      if (hdfs.exists(filePath))  hdfs.delete(filePath, true)
    }
  }
}
