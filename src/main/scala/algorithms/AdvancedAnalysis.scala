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
import scala.collection.mutable.ListBuffer

class AdvancedAnalysis(spark:SparkSession, postingListDf:DataFrame) {
  import spark.implicits._

  //var nDoc:Long = 0

  // distinctPmids
  def docNumber(): Long = {

    //return postingListDf.as[PostingList].map(attrs => (attrs.pmid, 0)).rdd.reduceByKey(_ + _).count
    return postingListDf.select("pmid").distinct.count
  }

  def idf(nDoc:Long): DataFrame = {

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

  def currMeshFreq(currPmidList:List[Int], meshDf:DataFrame, fuzzyDf:DataFrame):DataFrame = {

    val conn = new Connector()
    //val currAbsList = ListBuffer[(Int, String)]
    //val oneAbs = conn.runOneAbs(onePmid).trim
    //val oneTopicDf = oneTopicList.toDF("mesh")

    //val oneTopicList = conn.runOneTopic(onePmid)
    val currAbsList   = currPmidList.map(pmid => (pmid, conn.runOneAbs(pmid)))
    val currTopicList = currPmidList.map(pmid => (pmid, conn.runOneTopic(pmid))) .flatMap({case (pmid:Int, arrayTopic:List[String]) => arrayTopic.map(topic => (pmid, topic)) })
    val currAbsDf =   currAbsList.toDF(  "pmid", "abs")
    val currTopicDf = currTopicList.toDF("pmid", "mesh")

    val resultDf = currAbsDf.alias("abs")
      .join(currTopicDf.alias("topic"), $"topic.pmid" === $"abs.pmid")
      .select($"abs.pmid", $"abs.abs", $"topic.mesh")
      .alias("mesh")
      .join(fuzzyDf.alias("fuzzy"), $"mesh.mesh" === $"fuzzy.mesh")
      .select($"mesh.pmid", $"mesh.abs", $"mesh.mesh", $"fuzzy.fuzzy")
      .map(attrs => (
        (attrs.getAs[String]("mesh"),
         attrs.getAs[Int]("pmid")), /*keys*/
        SerializableClass.countSubstring(attrs.getAs[String]("abs"),
         attrs.getAs[String]("fuzzy")) /*value*/
      ))
      .rdd.reduceByKey(_ + _)
      .map({
        case ((mesh, pmid), count) =>
        (mesh, pmid, count)
      }).toDF("mesh", "pmid", "count")

    /*
    //val focusedFuzzy = fuzzyDf.alias("a").join(oneTopicDf.alias("b"), $"a.mesh" === $"b.mesh", "inner").select($"a.mesh", $"a.fuzzy")
    val resultDf = fuzzyDf
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
    */

    return resultDf
  }

  def similarity(
    currPmidList:List[Int], 
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
    val currLen = currPmidList.map(pmid => (pmid, conn.runOneAbs(pmid).split(" ").size))
    //val currLen = conn.runOneAbs(onePmid).split(" ").size
    val currLenDf = currLen.toDF("pmid", "length")
    //val currLenDf = List((onePmid, currLen)).toDF("pmid", "length")
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

  def sumSimilarity(similarityDf:DataFrame):DataFrame = {
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
      .toDF("curpmid", "relpmid", "score")
    return sumSimilarityDf
  }

  def main(currPmidList:List[Int], absDf:DataFrame, meshDf:DataFrame, fuzzyDf:DataFrame, nDoc:Long):DataFrame = {
    val currMeshFreqDf = currMeshFreq(currPmidList, meshDf, fuzzyDf)
    val lenDf = absLength(absDf)
    //val nDoc = docNumber()
    val idfDf = idf(nDoc)
    val similarityDf = similarity(currPmidList, currMeshFreqDf, lenDf, idfDf)

    return similarityDf
  }

  def rankAndCollect(sumSimilarityDf:DataFrame):Array[(Int, List[(Int, Double)])]= {
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.{rank, desc}

    val w = Window.partitionBy($"curpmid").orderBy(desc("score"))
    val ranked = sumSimilarityDf
      .withColumn("rank", rank.over(w))
      .where($"rank" <= 10)

      /*
+--------+--------+------------------+----+
| curpmid| relpmid|             score|rank|
+--------+--------+------------------+----+
       * */

    val rankedSimilarResult = ranked.map(
      attrs => (attrs.getAs[Int]("curpmid"), /* key */
        List((attrs.getAs[Int]("relpmid")
          , attrs.getAs[Double]("score"))) /* value */
        )).rdd
      .reduceByKey(_ ++ _).collect

    return rankedSimilarResult
  }

  def loadRelatedMeshTable():DataFrame = {
    spark.read.load("./data/related_mesh_table.parquet")
  }

}
