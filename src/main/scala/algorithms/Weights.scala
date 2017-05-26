package algorithms

import scala.collection._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
class Weights(spark:SparkSession) extends java.io.Serializable {
  import spark.implicits._

  def scoreVec(termFreqs:Map[String, Int], idfs:Map[String, Double], termIds:immutable.Map[String, Int]):Seq[(Int, Double)] = {
    val eta = 1.0
    val mu = 0.013
    val lambda = 0.022
    val absTotalTerms = termFreqs.values.sum
    val l = absTotalTerms

    val termScores = termFreqs.filter{
	case (term, freq) => termIds.contains(term)
      }.map { case (term, freq) => {
      val km1 = termFreqs(term).toDouble - eta
      val idf = idfs(term)

      val w = math.sqrt(idf) /
	  (
	    1.0 +
	    math.pow( (mu / lambda), km1) *
	    math.exp(  -(mu - lambda) * l)
	  )
      (termIds(term), w)
    }}.toSeq
    termScores
  }

  def scoreMat(termFreqs:Map[String, Int], rowIdx:Int, idfs:Map[String, Double], termIds:immutable.Map[String, Int]):Seq[(Int, Int, Double)] = {

    val eta = 1.0
    val mu = 0.013
    val lambda = 0.022
    val absTotalTerms = termFreqs.values.sum
    val l = absTotalTerms
    val termSize = termIds.size

    val termScores = termFreqs.filter{
	case (term, freq) => termIds.contains(term)
      }.map { case (term, freq) => {
      val km1 = termFreqs(term).toDouble - eta
      val idf = idfs(term)

      val w = math.sqrt(idf) /
	  (
	    1.0 +
	    math.pow( (mu / lambda), km1) *
	    math.exp(  -(mu - lambda) * l)
	  )
      (termIds(term), rowIdx, w)
    }}.toSeq

    termScores
  }

  /*
   * Input: [Int, Map[String, Int]]
   * */
  def scoreMatByDocs(
    tfByDocs:DataFrame
    , idfs:Map[String, Double]
    , termIds:immutable.Map[String, Int]
  ): DataFrame= {
    val mat = tfByDocs.flatMap(attrs => {
      val pmid = attrs.getInt(0)
      val termFreqs = attrs.getMap[String, Int](1)
      scoreMat(termFreqs, pmid, idfs, termIds)
    }).toDF("term_id", "pmid", "weight")
    return mat
    //.toDF("term_id", "pmid", "score")
  }
}
