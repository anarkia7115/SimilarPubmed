package algorithms

import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Matrices
import scala.collection.SortedSet


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
  //val cmat = new RowMatrix(vecs)

  //val k = 1000
  //val svd = cmat.computeSVD(k, computeU=true)
  //val us = multiplyByDiagonalRowMatrix(svd.U, svd.s)

  def matToVecs(mat:DataFrame, termSize:Int): 
    RDD[IndexedRow] = {

    val docTws = mat.map(attrs => {
      val term_id = attrs.getInt(0)
      val pmid = attrs.getInt(1)
      val weight = attrs.getDouble(2)
      (pmid, Seq((term_id, weight)))
    }).rdd.reduceByKey(_ ++ _).persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

    val vecs = docTws.map {
      case (pmid, twList) => {
        new IndexedRow(pmid, Vectors.sparse(termSize, twList))
      }
    }
    return vecs
  }
  def run(us:RDD[IndexedRow], pmidRdd:RDD[Long], topResultsOut:String): Unit = {
    val topResults: Seq[(Long, Long, Double)] = 
      topDocsForDoc(us, pmidRdd)
    val sc = spark.sparkContext
    sc.parallelize(topResults
      ).map{ case (src_pmid, rel_pmid, score) => {
        "%s\t%s\t%s".format(src_pmid, rel_pmid, score)
      }}.saveAsTextFile(topResultsOut)
  }

  def calcOnePmidTopResult(us:RDD[IndexedRow], onePmid:Int, topResultsOut:String): Unit = {
    val topResults: Seq[(Long, Double)]  = 
      topDocsForDoc(us, onePmid.toLong)
    val sc = spark.sparkContext
    sc.parallelize(topResults
      ).map{case (pmid, score) => {
        "%s\t%s".format(pmid, score)
      }}.saveAsTextFile(topResultsOut)
  }

  def calcUs(
    svd:SingularValueDecomposition[IndexedRowMatrix, Matrix] 
  , resultPath:String
  ): Unit = {
    val uIdxMat = svd.U
    val sVec = svd.s
    val us = multiplyByDiagonalRowMatrix(uIdxMat, sVec)
    us.rows.saveAsObjectFile(resultPath)
  }

  def calcUs(uRows:RDD[IndexedRow], sRdd:RDD[Double]): Unit = {
    val uIdxMat = new IndexedRowMatrix(uRows)
    val sVec = Vectors.dense(sRdd.collect)
    val us = multiplyByDiagonalRowMatrix(uIdxMat, sVec)
    us.rows.saveAsObjectFile("hdfs://soldier1:9000/data/svd/us.bin")
  }

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

  def multiplyByDiagonalRowMatrix(mat: IndexedRowMatrix, diag: Vector): IndexedRowMatrix = {
    val sArr = diag.toArray
    new IndexedRowMatrix(mat.rows.map { idxRow =>
      val idx = idxRow.index
      val vec = idxRow.vector
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      new IndexedRow(idx, Vectors.dense(newArr))
    })
  }

  def multiplyByDiagonalRowMatrix(mat: RowMatrix, diag: Vector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map { vec =>
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    })
  }

  def distributedRowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map { vec =>
      val array = vec.toArray
      val length = math.sqrt(array.map(x => x * x).sum)
      Vectors.dense(array.map(_ / length))
    })
  }

  def topDocsForDoc(us:RowMatrix, docId: Long): Seq[(Double, Long)] = {
    // Look up the row in US corresponding to the given doc ID.
    //val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docId).head.toArray
    val docRowArr = us.rows.zipWithUniqueId.map(_.swap).lookup(docId).head.toArray
    val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)

    // Compute scores against every doc
    val docScores = us.multiply(docRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId

    // Docs can end up with NaN score if their row in U is all zeros.  Filter these out.
    allDocWeights.filter(!_._1.isNaN).top(10)
  }

  def topDocsForDoc(us:RDD[IndexedRow], pmid: Long): Seq[(Long, Double)] = {
    // Look up the row in US corresponding to the given doc ID.
    //val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docId).head.toArray
    us.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    val docRowArr = us.map(ir => (ir.index, ir.vector)).lookup(pmid).head.toArray
    val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)

    // Compute scores against every doc
    val usMat = new IndexedRowMatrix(us)
    val docScores:IndexedRowMatrix = usMat.multiply(docRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(ir => {
      (ir.index, ir.vector.toArray(0))
    })
    val ordering = Ordering.by[(Long, Double), Double](_._2)

    // Docs can end up with NaN score if their row in U is all zeros.  Filter these out.
    allDocWeights.filter(!_._2.isNaN).top(100)(ordering)
  }

  def topDocsForDoc(us:RDD[IndexedRow], pmidRdd: RDD[Long]): 
    Seq[(Long, Long, Double)] = {
    // Look up the row in US corresponding to the given doc ID.
    //val docRowArr = normalizedUS.rows.zipWithUniqueId.map(_.swap).lookup(docId).head.toArray
    us.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
    val pmidSet = pmidRdd.collect.toSet
    val sc = spark.sparkContext
    val bPmidSet = sc.broadcast(pmidSet).value
    //val docRowArr = us.map(ir => (ir.index, ir.vector)).lookup(pmid).head.toArray
    val docRowArrDf = us.flatMap(ir => {
      val pmid:Long = ir.index
      val vec:Vector = ir.vector
      if (bPmidSet.contains(pmid)) {
        Some((pmid, vec))
      } else {
        None
      }
    }).toDF("pmid", "vec")
    // [(Long, Vector)]
    docRowArrDf.write.save("hdfs://soldier1:9000/svd/docRowArrDf")
    val docRowArr = docRowArrDf.as[(Long, Vector)].collect.toArray
    val pmidArr:Array[Long] = docRowArr.map(_._1)
    val vecArr:Array[Vector] = docRowArr.map(_._2)
    val oneLineVecArray = vecArr.foldLeft(
      Array.emptyDoubleArray)(_.toArray++_.toArray)
    val rowSize = vecArr(0).size
    val colSize = pmidArr.size
    val bPmidArr = sc.broadcast(pmidArr).value
    val docRowVecs = Matrices.dense(rowSize, colSize, oneLineVecArray)

    // Compute scores against every doc
    val usMat = new IndexedRowMatrix(us)
    val docScores:IndexedRowMatrix = usMat.multiply(docRowVecs)

    val topSize = 101
    // [rel_pmid, score]
    val sortedSetOrdering = Ordering.by[(Long, Double), Double](_._2)
    val topScores:List[SortedSet[(Long, Double)]] = 
      docScores.rows.aggregate[List[SortedSet[(Long, Double)]]](
      List.fill(colSize)(SortedSet.empty(sortedSetOrdering))
    )(
      algorithms.SerializableClass.aggSeqOp(topSize, sortedSetOrdering)
    , algorithms.SerializableClass.aggCombOp(topSize)
    )
    topScores.zip(bPmidArr).flatMap{ case (ss, src_pmid) => {
      ss.map{ case (rel_pmid, score) => {
        (src_pmid, rel_pmid, score)
      }}
    }}.toSeq
  }
}
