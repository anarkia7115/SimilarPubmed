package render

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/*
 * Input: [pmid title absText]
 * */
class CleanDocs(spark:SparkSession) {

  def getRdd(inputRDD:RDD[String]):RDD[(Int, String, String)] = {
    val sc = spark.sparkContext
    val xtr = new topic.XmlTagRemover("AbstractText")
    val textRdd = inputRDD
    val cleanRdd = textRdd.flatMap { line =>
      val fields = line.trim.split('\t')
      if (fields.size != 3) {
        None
      } else {
        val (pmid, title, absText) = (fields(0).toInt, fields(1), xtr.trim(fields(2)))
        Some((pmid, title, absText))
      }
    }
    return cleanRdd
  }

  def getRdd(inputPath:String):RDD[(Int, String, String)] = {
    val sc = spark.sparkContext
    val xtr = new topic.XmlTagRemover("AbstractText")
    val textRdd = sc.textFile(inputPath)
    val cleanRdd = textRdd.flatMap { line =>
      val fields = line.trim.split('\t')
      if (fields.size != 3) {
        None
      } else {
        val (pmid, title, absText) = (fields(0).toInt, fields(1), xtr.trim(fields(2)))
        Some((pmid, title, absText))
      }
    }
    return cleanRdd
  }

}
