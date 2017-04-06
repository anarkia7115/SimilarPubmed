package algorithms

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import topic.Connector
import sparkutil.MeshHeading
import sparkutil.MeshFuzzy
import sparkutil.PostingList
import sparkutil.Abstract
import scala.math

class HigherAnalysis(spark:SparkSession, postingListDf:DataFrame) {
  import spark.implicits._

  // distinctPmids
  def docNumber(): Long = {

    return postingListDf.as[PostingList].map(attrs => (attrs.pmid, 0)).rdd.reduceByKey(_ + _).count
  }

  def idf(nDoc:Double): DataFrame = {
    val idfDf = postingListDf.as[PostingList]
      .map(attrs => (attrs.mesh, 1))
      .rdd.reduceByKey(_ + _)
      .toDF("mesh", "freq")
      .map(attrs => (attrs.getAs[String]("mesh"), math.log(nDoc / attrs.getAs[Int]("freq"))))
      .toDF("mesh", "idf")

    return idfDf
  }

  def absLength(absDf:DataFrame): DataFrame = {
    val lengthDf = absDf.as[Abstract]
      .map(attrs => (attrs.pmid, attrs.abs.split(' ').size))
      .toDF("pmid", "length")
    return lengthDf
  }

  def currMeshFreq(onePmid:Int, meshDf:DataFrame, fuzzyDf:DataFrame):DataFrame = {

    val conn = new Connector()
    val oneAbs = conn.runOneAbs(onePmid).trim
    val oneTopicList = conn.runOneTopic(onePmid)
    val oneTopicDf = oneTopicList.toDF("mesh")
    val focusedFuzzy = fuzzyDf.alias("a").join(oneTopicDf.alias("b"), $"a.mesh" === $"b.mesh", "inner").select($"a.mesh", $"a.fuzzy")
    val resultDf = focusedFuzzy
      .map(attr => (
        (onePmid
        , attr.getAs[String]("mesh"))
        , SerializableClass.countSubstring(oneAbs, 
          attr.getAs[String]("fuzzy"))
        ))
      .rdd.reduceByKey(_ + _)
      .map({
        case ((pmid, mesh), count) =>
        (pmid, mesh, count)
      })
      .toDF("pmid", "mesh", "count")

    return resultDf
  }

  def similarity(
    onePmid:Int, 
    currMeshFreqDf: DataFrame, 
    lenDf: DataFrame,
    idfDf: DataFrame
    ):DataFrame = {

    currMeshFreqDf.createOrReplaceTempView("currPostingList")
    //postingListDf.join(currMeshFreqDf, currMeshFreqDf("mesh") === postingListDf("mesh"))
    postingListDf.createOrReplaceTempView("bigPostingList")
    lenDf.createOrReplaceTempView("abslength")
    idfDf.createOrReplaceTempView("meshidf")

    val conn = new topic.Connector()
    val currLen = conn.runOneAbs(onePmid).split(" ").size
    val currLenDf = List((onePmid, currLen)).toDF("pmid", "length")
    currLenDf.createOrReplaceTempView("currabslength")
    /*
    val currDataDf = 
      currMeshFreqDf.alias("ps")
      .join(currLenDf.alias("ls"), $"ps.pmid" === $"ls.pmid")
      .join(idfDf.alias("is"), $"ps.mesh" === $"is.mesh")
      .select($"ps.mesh", $"ps.pmid", $"ps.count", $"ls.length", $"is.idf")
      .toDF("mesh", "pmid", "count", "len", "idf")
      .alias("curdf")
      .join(
        postingListDf.alias("pb")
        .join(lenDf.alias("lb"), $"pb.pmid" === $"lb.pmid")
        .join(idfDf.alias("ib"), $"pb.mesh" === $"ib.mesh")
        .select($"pb.mesh", $"pb.pmid", $"pb.count", $"lb.length", $"ib.idf")
        .toDF("mesh", "pmid", "count", "len", "idf")
        .alias("reldf")
        , $"reldf.mesh" === $"curdf.mesh")
      .select($"curdf.mesh"
        , $"curdf.pmid"
        , $"reldf.pmid"
        , $"curdf.count"
        , $"reldf.count"
        , $"curdf.len"
        , $"reldf.len"
        , $"curdf.idf"
        , $"reldf.idf")
      .toDF("mesh"
        , "curpmid"
        , "relpmid"
        , "curcount"
        , "relcount"
        , "curlen"
        , "rellen"
        , "curidf"
        , "relidf")
    */

    val q1 = """
      SELECT ps.mesh
      , pb.pmid as relpmid
      , ps.count as curcount
      , pb.count as relcount
      from currPostingList ps
      join bigPostingList pb
        on ps.mesh = pb.mesh
    """

    val query = """
      SELECT ps.mesh
      , ps.pmid as curpmid
      , pb.pmid as relpmid
      , ps.count as curcount
      , pb.count as relcount
      , ls.length as curlen
      , lb.length as rellen
      , is.idf as curidf
      , ib.idf as relidf
      from currPostingList ps
      join bigPostingList pb
        on ps.mesh = pb.mesh
      join currabslength ls
        on ps.pmid = ls.pmid
      join abslength lb
        on pb.pmid = lb.pmid
      join meshidf is
        on ps.mesh = is.mesh
      join meshidf ib
        on pb.mesh = ib.mesh
    """
    val currDataDf = spark.sql(query)
    val similarityDf = currDataDf.map(attrs => (
      attrs.getAs[String]("mesh"),
      attrs.getAs[Int]("curpmid"),
      attrs.getAs[Int]("relpmid"),
      SerializableClass.singleWeight(
        attrs.getAs[Int]("curcount"),
        attrs.getAs[Int]("curlen"),
        attrs.getAs[Double]("curidf")
      ),
      SerializableClass.singleWeight(
        attrs.getAs[Int]("relcount"),
        attrs.getAs[Int]("rellen"),
        attrs.getAs[Double]("relidf")
      )
    )).toDF("mesh", "curpmid", "relpmid", "curscore", "relscore")

    return similarityDf
  }

  def sumSimilarity(similarityDf):DataFrame = {
    val sumSimilarityDf = similarityDf.map(attrs => (
      (attrs.getAs[Int]("curpmid"),
      attrs.getAs[Int]("relpmid")), /* key */
      attrs.getAs[Double]("curscore")*
      attrs.getAs[Double]("relscore") /* value */
    )).rdd.reduceByKey(_ + _)
      .map({ case
        ((curpmid, relpmid), score) =>
        (curpmid, relpmid, score)
      })
      .toDF("curpmid", "redpmid", "score")
    return sumSimilarityDf
  }

  def main(onePmid:Int, absDf:DataFrame, meshDf:DataFrame, fuzzyDf:DataFrame):DataFrame = {
    val currMeshFreqDf = currMeshFreq(onePmid, meshDf, fuzzyDf)
    val lenDf = absLength(absDf)
    val nDoc = docNumber()
    val idfDf = idf(nDoc)
    val similarityDf = similarity(onePmid, currMeshFreqDf, lenDf, idfDf)

    return similarityDf
  }

}
