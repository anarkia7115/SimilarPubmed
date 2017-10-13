package utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.collection._

class DataWriter(spark:SparkSession) {
  import spark.implicits._

  def write(m:Map[String, Double], hdfsPath:String): Unit = {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(
      m.toSeq
    )
    rdd.toDF("key", "value").write.save(hdfsPath)
  }

  def write(m:immutable.Map[String, Int], hdfsPath:String): Unit = {
    val sc = spark.sparkContext
    val rdd = sc.parallelize(
      m.toSeq
    )
    rdd.toDF("key", "value").write.save(hdfsPath)
  }

}
