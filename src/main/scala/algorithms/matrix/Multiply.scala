package algorithms.matrix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class Multiply {

// RDD x Seq
def coordinateMatrixMultiply(leftMatrix: RDD[(Int, Int, Double)], rightMatrix: Seq[(Int, Int, Double)]): RDD[((Int, Int), Double)] = {
  val L_ = leftMatrix.map{  case (i, j, v) => (j, (i, v)) }
  val R_ = sc.broadcast(
    rightMatrix.map{ case (j, k, w) => (j, (k, w)) }.toMap
  )

  val productEntries = L_
    .flatMap{ case (j, (i, v)) => {
      if (R_.value.contains(j)) {
        Some((i, v), R_.value(j))
      }
      else {
        None
      }
    }}
    .map{case ((i, v), (k, w)) => ((i, k), (v * w))}
    .reduceByKey(_ + _)

  productEntries
}

// RDD x RDD
def coordinateMatrixMultiply(
    leftMatrix:  RDD[(Int, Int, Double)]
  , rightMatrix: RDD[(Int, Int, Double)]): RDD[((Int, Int), Double)] = {

  val L_ = leftMatrix.map{  case (i, j, v) => (j, (i, v)) }
  val R_ = rightMatrix.map{ case (j, k, w) => (j, (k, w)) }

  val productEntries = L_
    .join(R_)
    .map{case (_, ((i, v), (k, w))) => ((i, k), (v * w))}
    .reduceByKey(_ + _)

  productEntries
}

// DF x DF
def coordinateMatrixMultiply(
    leftMatrix:  DataFrame
  , rightMatrix: DataFrame): DataFrame = {

  val L_ = leftMatrix.toDF("pmid", "term_id", "weight")
  // (pmid, termId, weight)
  val R_ = rightMatrix.toDF("term_id", "pmid", "weight")
  // (termId, pmid, value)

  val productEntries = L_.as("l")
    .join(R_.as("r"), "term_id")
    .select("l.pmid", "l.weight", "r.pmid", "r.weight")
    .map(attrs => (
      (attrs.getInt(0), attrs.getInt(2)), // key
      (attrs.getDouble(1) * attrs.getDouble(3))
      ))
    .rdd.reduceByKey(_ + _)
    .map{case ((p1, p2), v) => (p1, p2, v)}.toDF("pmid1", "pmid2", "weight")

  productEntries
}

}
