package algorithms.matrix

import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import topic.TermDocWeight

class Multiply(spark:SparkSession) {

  val sc = spark.sparkContext
  import spark.implicits._

  // RDD x Seq
  /*
  def coordinateMatrixMultiply(leftMatrix: DataFrame, rightMatrix: Seq[(Int, Int, Double)]): DataFrame = {
    val L_ = leftMatrix
    val R_ = sc.broadcast(
      rightMatrix.map{case (tid, pmid, weight) => {
        (pmid, (tid, weight))
      }}.foldLeft(Seq.empty[(Int, Map[Int, Double])]){ case (m, (pmid, tw)) => {
        val twBypmid = (m.getOrElse(pmid, Map.empty[Int, Double]) ++ tw)
        m + (pmid -> twBypmid)
      }}.toList
    )
    // List[(pmid, term_id -> weight)]

    val productEntries = L_.flatMap( attrs => {
        val j = attrs.getInt(0)
        val i = attrs.getInt(1)
        val v = attrs.getDouble(2)
        R_.value.flatMap{ case (pmid, tw) => {
          if (tw.contains(j)) {
            Some((i, v), (pmid, tw(j)))
          }
          else {
            None
          }
        }}})
      .map{case ((i, v), (k, w)) => ((i, k), (v * w))}
      .rdd.reduceByKey(_ + _)
      .map{case ((p1, p2), v) => (p1, p2, v)}.toDF("rel_pmid", "src_pmid", "weight")


    import algorithms.Rank

    val rk = new Rank()

    val ranked = rk.rank(productEntries, "src_pmid", "weight").filter($"rank" <= 101)

    ranked
  }
  */

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
  def coordinateMatrixMultiply2(
      leftMatrix:  DataFrame
    , rightMatrix: DataFrame): DataFrame = {

    val L_ = leftMatrix.toDF("pmid", "term_id", "weight")
    // (pmid, termId, weight)
    val R_ = rightMatrix.toDF("term_id", "pmid", "weight")
    // (termId, pmid, value)
    import spark.implicits._

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

  /*
  // DF x DF
  def coordinateMatrixMultiply3(
      leftMatrix:  DataFrame
    , rightMatrix: DataFrame): DataFrame = {

    val L_ = leftMatrix
    // (pmid, term_id, weight)
    val R_ = rightMatrix
    // (term_id, pmid, value)
    import spark.implicits._

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
  */

  // DS x DS
  def coordinateMatrixMultiply3(
      leftMatrix:  Dataset[TermDocWeight]
    , rightMatrix: Dataset[TermDocWeight]): DataFrame = {

    val L_ = leftMatrix
    // (pmid, term_id, weight)
    val R_ = rightMatrix
    // (term_id, pmid, value)
    import spark.implicits._

    val productEntries = L_.as("l")
      .join(R_.as("r"), "term_id")
      .select("l.pmid", "l.weight", "r.pmid", "r.weight")
      .map(attrs => (
        (attrs.getInt(0), attrs.getInt(2)), // key
        (attrs.getDouble(1) * attrs.getDouble(3))
        ))
      .rdd.reduceByKey(_ + _)
      .map{case ((p1, p2), v) => (p1, p2, v)}.toDF("src_pmid", "rel_pmid", "weight")

    import algorithms.Rank

    val rk = new Rank[Row]()

    val ranked = rk.rank(productEntries, "src_pmid", "weight").filter($"rank" <= 101)

    ranked
  }
}
