package utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class DataLoader(spark:SparkSession) {

  val hdfsRoot = "hdfs://hpc2:9000"

  def load(hdfsPath:String):DataFrame = {
    return spark.read.load(hdfsRoot + hdfsPath)
  }
}
