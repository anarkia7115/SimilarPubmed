package algorithms

import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.collection._

// Row(pmid, Seq[terms])
//class Terms(rdd:RDD[(Int, Seq[String])]) {
class Terms(spark:SparkSession, termsDf:DataFrame) {
  import spark.implicits._

  /*
   * Term Frequency Map for every doc.
   *
   * */
  def tfByDoc(): DataFrame = {

    //termsDf.map{ case (pmid, terms) => {
    val resultDf = termsDf.map(attrs => {
      // init an empty TF map
      val pmid = attrs.getInt(0)
      val terms = attrs.getSeq[String](1)
      val tf = terms.foldLeft(new HashMap[String, Int]()) {
        (map, term) => {
          map += term -> (map.getOrElse(term, 0) + 1)
          map
        }
      }
      // tf by doc
      (pmid, tf.toMap)
    }).toDF("pmid", "term_freqs")

    return resultDf
  }

  /*
   * Every term count once per docs, 
   * even if the term occurs multiple 
   * times in the same doc.
   *
   * */
  /*
  def docFreqs(): DataFrame = {
    // calc from origin rdd
    rdd.fold(new HashMap[String, Int]()) {
      case (map, (pmid, terms)) => {
        // distinct occurences
        terms.toSet.foreach( term => {
          map += term -> map.getOrElse(term, 0) + 1
        })
      }
    }
  }
  */

  def docFreqs(tfbd:DataFrame):DataFrame = {
    // calc from tfByDoc Df
    tfbd.flatMap(_.getMap[String, Int](1).keySet).map((_, 1)).rdd.reduceByKey(_ + _).toDF("term", "count")
  }

  /*
   * Inverse Document Frequency
   * Need to calculate Document Frequency(docFreqs) 
   * first.
   * Before doing this, docFreqs should be limited 
   * by freqs, freqs too low should be discard
   *
   * */
  def topIdf(docf:DataFrame, topSize:Int): Map[String, Double] = {

    // docf: [term, count]
    val ordering = Ordering.by[(String, Int), Int](_._2)
    val filtered = docf.as[(String, Int)].rdd.top(topSize)(ordering)
    // number of all documents
    val numDocs = termsDf.count

    filtered.map ( attrs => {
      val term = attrs._1
      val count = attrs._2
      // idf
      (term, math.log(numDocs.toDouble / count))
    }).toMap
  }

  def idf(docf:DataFrame, freqThreshold:Int): Map[String, Double] = {

    val filtered = docf.filter($"count" >= freqThreshold)
    // number of all documents
    val numDocs = termsDf.count

    filtered.map ( attrs => {
      val term = attrs.getString(0)
      val count = attrs.getInt(1)
      // idf
      (term, math.log(numDocs.toDouble / count))
    }).rdd.collectAsMap
  }

  /*
  def idf(freqThreshold:Int): Map[String, Double] = {
    val tfbd = tfByDoc()
    val docf = docFreqs(tfbd)
    // number of all documents
    idf(filteredDocF, freqThreshold)
  }
  */
}
