package algorithms

import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

class Svd(spark:SparkSession) {
  import spark.implicits._

  /*
  def findImportantConcepts(svd:SingularValueDecomposition, numConcepts:Int, termIds:Map[Int, String]): Array[Seq[(String, Double)]] = {
    import scala.collection.mutable.ArrayBuffer
    
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    val vnr = v.numRows
    val numTerms = v.numCols

    for (i <- 0 until numConcepts) {
      val offs = i * vnr
      val termWeights = arr.slice(offs, offs + vnr).zipWithIndex
      val sorted = termWeights.sortBy(- _._1)
      topTerms += sorted.take(numTerms).map{
        case (score, id) => (termIds(id), score)
      }
    }
    topTerms.toArray
  }
  */

  def genRowMatrix(mat:DataFrame): RowMatrix = {
    /*before this function cache `mat` first*/
    val docIdDf = indexPmid(mat)
    val ijm = ijMat(mat)
    val vecs = lDf2Vecs(ijm)
    val numTerms = mat.select("term_id").as[Int].rdd.max() + 1
    val numPmid = mat.select("pmid").distinct.count.toInt
    val cooMat = new CoordinateMatrix(vecs, numPmid, numTerms)
    val termDocMat = cooMat.toRowMatrix
    return termDocMat
  }

  /*
   * Input:  (term_id, pmid,    weight)
   * Output: (doc_id,  term_id, weight)
   * */
  def ijMat(mat: DataFrame): DataFrame = {
    val docIdDf = indexPmid(mat)
    val ijMat = mat.join(docIdDf, "pmid").select("doc_id", "term_id", "weight")
    ijMat
  }

  /*(term_id, pmid, weight)*/
  def indexPmid(mat: DataFrame): DataFrame = {
    val docIdDf = mat.select("pmid").as[Int].
      distinct.rdd.zipWithIndex.toDF("pmid", "doc_id")
    return docIdDf
  }

  /*(docId, termId, weight)*/
  def lDf2Vecs(mat:DataFrame):RDD[MatrixEntry] = {
    val lMatEntries = mat.map(attrs => {
      MatrixEntry(attrs.getLong(0).toInt, attrs.getInt(1), attrs.getDouble(2))
    }).rdd
    return lMatEntries
  }
}
