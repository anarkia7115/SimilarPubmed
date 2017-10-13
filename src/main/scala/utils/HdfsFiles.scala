package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class HdfsFiles(spark:SparkSession, hdfsRoot:String) {

  def exists(filePath:String) : Boolean = {
    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    conf.set("fs.defaultFS", hdfsRoot)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(conf)
    hdfs.exists(new org.apache.hadoop.fs.Path(filePath))
  }

}
