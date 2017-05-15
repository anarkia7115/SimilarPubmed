package render

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/*
 * Input: [pmid title absText]
 * */
class CleanDocs(inputPath:String) {

  def getRdd(sc:SparkContext):RDD = {
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
  }

}
