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
    //去掉中括号
    //rdd.map(s=>(s(0),s(1))).map(_.productIterator.mkString(",")).saveAsTextFile("/user/hadoop/input/bb")
    scala.tools.nsc.io.File(path).appendAll("\n" + rdd.collect().mkString("\n"))
  }

  def writeToParquetFile(data: DataFrame, path: String): Unit = {
    data.write.mode(SaveMode.Append).format("parquet").save("path")
  }

  def deleteHdfsFile(path: String) = {
    val conf: Configuration = new Configuration()
    val filePath = new Path(path)
    val hdfs = filePath.getFileSystem(conf)
    //检查是否存在文件
    if (hdfs.exists(filePath)) {
      // 有则删除
      hdfs.delete(filePath, true)
    }
  }
}
